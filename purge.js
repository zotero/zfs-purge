/*
 ***** BEGIN LICENSE BLOCK *****
 
 This file is part of the Zotero Data Server.
 
 Copyright © 2018 Center for History and New Media
 George Mason University, Fairfax, Virginia, USA
 http://zotero.org
 
 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Affero General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.
 
 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU Affero General Public License for more details.
 
 You should have received a copy of the GNU Affero General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 
 ***** END LICENSE BLOCK *****
 */

const zlib = require('zlib');
const parse = require('csv-parse/lib/sync');
const config = require('config');
const AWS = require('aws-sdk');
const mysql = require('mysql2/promise');
const fs = require('fs');

const S3ZFS = new AWS.S3(config.get('S3ZFS'));
const S3Inventory = new AWS.S3(config.get('S3Inventory'));

let db = null;

let numDeleted = 0;
let CSVCount = 0;
let CSVCurrent = 0;
let CSVLines = 0;
let CSVLinesProcessed = 0;

/**
 * Get all keys by prefix.
 * One request can return only a limited count of keys,
 * therefore do recursion with continuation token
 * @param prefix
 * @param token
 * @return {Promise<S3.ObjectList>}
 */
async function getList(prefix, token) {
	let params = {Prefix: prefix};
	if (token) params.ContinuationToken = token;
	let result = await S3Inventory.listObjectsV2(params).promise();
	let list = result.Contents;
	if (result.IsTruncated) {
		list = list.concat(await getList(prefix, result.NextContinuationToken));
	}
	return list;
}

/**
 * Get a manifest file containing a list of all CSVs
 * @return {Promise<*>}
 */
async function getManifest() {
	// List keys that only start with a date
	let prefix = config.get('S3ZFS').params.Bucket + '/' + config.get('inventoryId') + '/20';
	
	let list = await getList(prefix);
	
	list = list.map(x => x.Key).filter(x => x.slice(-13) === 'manifest.json');
	list.sort().reverse();
	
	if (!list.length) return null;
	
	let manifestKey = list[0];
	
	let result = await S3Inventory.getObject({Key: manifestKey}).promise();
	let manifest = result.Body.toString();
	manifest = JSON.parse(manifest);
	return manifest;
}

/**
 * Process CSV file.
 * Assumes that keys are sorted in CSV, combines the same hash keys
 * into groups, and runs purge operation on that group.
 * This function processes one CSV at the time, which means a group,
 * that stretches over two files, will result to two partial groups.
 *
 * Example of a group:
 * 697ffb8a8eda3418ceb6cbf5f2005c63
 * 697ffb8a8eda3418ceb6cbf5f2005c63/Some article.pdf
 * 697ffb8a8eda3418ceb6cbf5f2005c63/Some article(1).pdf
 * 697ffb8a8eda3418ceb6cbf5f2005c63/c/SNPHECC9.zip
 * 697ffb8a8eda3418ceb6cbf5f2005c63/c/EHHNB7WA.zip
 *
 * @param key
 * @return {Promise<void>}
 */
async function processCSV(key) {
	let result = await S3Inventory.getObject({Key: key}).promise();
	let csv = zlib.gunzipSync(result.Body);
	let rows = parse(csv);
	
	CSVLines = rows.length;
	
	let group = [];
	for (let row of rows) {
		CSVLinesProcessed++;
		let key = row[1];
		let hash = key.split('/')[0];
		
		// Make sure the key starts with a hash
		if (!/^[a-z0-9]{32}$/.test(hash)) continue;
		
		// Separate and purge the group if the current hash isn't equal to the group hash
		if (group.length && group[0].split('/')[0] !== hash) {
			// Process the group
			await purgeGroup(group);
			// Start a new group
			group = [];
		}
		
		group.push(key);
	}
	
	if (group.length) {
		await purgeGroup(group);
	}
}

/**
 * Delete objects from s3.
 * S3 API can delete up to 1000 keys per request,
 * therefore we do recursion here
 * @param keys
 * @return {Promise<void>}
 * @constructor
 */
async function S3Delete(keys) {
	let partKeys = keys.splice(0, 1000);
	
	let objects = partKeys.map(key => ({Key: key}));
	
	let params = {
		Delete: {
			Objects: objects
		}
	};
	
	await S3ZFS.deleteObjects(params).promise();
	
	if (keys.length) {
		await S3Delete(keys);
	}
}

async function purgeGroup(group) {
	let hash = group[0].split('/')[0];
	
	await db.beginTransaction();
	
	try {
		let res;
		
		// Selects and locks all storageUploadQueue table rows containing
		// the specific hash and initiated in the last month.
		// Prevents new rows creation
		[res] = await db.execute(`
			SELECT COUNT(*) AS count
			FROM storageUploadQueue
			WHERE hash = ?
			AND time > SUBDATE(CURDATE(), INTERVAL 1 MONTH)
			FOR UPDATE`,
			[hash]
		);
		
		// If at least one upload is initiated in the last month, we skip this hash
		if (res[0].count) {
			console.log('Skipping hash from storageUploadQueue: ' + hash);
			await db.commit();
			return;
		}
		
		// Check if the file is still in use and lock all storageFiles rows containing the hash
		[res] = await db.execute(`
			SELECT MAX(sfl.libraryID) AS libraryID
			FROM storageFiles sf
			LEFT JOIN storageFileLibraries sfl ON (sfl.storageFileID = sf.storageFileID)
			WHERE hash = ?
			FOR UPDATE`,
			[hash]
		);
		
		let using = true;
		// If none of libraries have the file, the file is no longer in use
		if (res[0].libraryID === null) {
			using = false;
		}
		
		let usedLastMonth = false;
		
		// If the file is no longer in use, check if it was used in the last month
		if (!using) {
			// Since all rows for the hash are already locked, we just check if at least
			// one storageFiles row was added/updated in the last month.
			let [res2] = await db.execute(`
			SELECT 1
			FROM storageFiles
			WHERE hash = ?
			AND lastAdded BETWEEN SUBDATE(CURDATE(), INTERVAL 1 MONTH) AND NOW()
			LIMIT 1`,
				[hash]
			);
			
			if (res2.length) {
				usedLastMonth = true;
			}
		}
		
		// DELETE EVERYTHING, if the file is not in use and haven't been used in the last month
		if (!using && !usedLastMonth) {
			let deleteKeys = group;
			
			numDeleted += deleteKeys.length;
			if (config.get('deletedLog')) {
				fs.appendFileSync(config.get('deletedLog'), deleteKeys.join('\n') + '\n\n');
			}
			
			if (config.get('actuallyDelete') === true) {
				// Backup storageFiles rows that will be deleted
				await db.execute(`
					REPLACE INTO storageFilesDeleted
					SELECT sf.*
					FROM storageFiles sf
					LEFT JOIN storageFileLibraries sfl ON (sfl.storageFileID = sf.storageFileID)
					WHERE hash = ? AND sfl.libraryID IS NULL`,
					[hash]
				);
				
				// Delete all storageFile rows that are no longer used in storageFileLibraries
				await db.execute(`
					DELETE sf
					FROM storageFiles sf
					LEFT JOIN storageFileLibraries sfl ON (sfl.storageFileID = sf.storageFileID)
					WHERE hash = ? AND sfl.libraryID IS NULL`,
					[hash]
				);
				
				// If rows are deleted from storageFiles, but fail to be deleted from S3,
				// nothing bad happens - the S3 files will be deleted the next time.
				// But if S3 deletion would succeed and storageFiles deletion would fail,
				// that would result to new uploads assuming that the file still exists in S3.
				// Therefore even if S3 fails (fail doesn't necessarily mean files aren't deleted),
				// we have to make sure the storageFiles deletion is committed.
				
				// await S3Delete(deleteKeys); <--- FOR NOW DO NOT DELETE THE ACTUAL FILES FROM S3
			}
		}
		// DELETE DUPLICATES
		else if (group.length >= 2) {
			// Only keep the shortest key. I.e. 'bucket/hash' if exists,
			// otherwise just 'bucket/hash/shortest_filename'
			group.sort((a, b) => a.length - b.length);
			let deleteKeys = group.slice(1);
			
			numDeleted += deleteKeys.length;
			if (config.get('deletedLog')) {
				fs.appendFileSync(config.get('deletedLog'), deleteKeys.join('\n') + '\n\n');
			}
			
			if (config.get('actuallyDelete') === true) {
				// await S3Delete(deleteKeys); <--- FOR NOW DO NOT DELETE THE ACTUAL FILES FROM S3
			}
		}
	}
	catch (err) {
		console.log(err);
	}
	
	await db.commit();
}

function printStatus() {
	console.log({
		CSV: CSVCurrent + '/' + CSVCount,
		CSVLine: CSVLinesProcessed + '/' + CSVLines,
		totalDeleted: numDeleted
	});
}

async function main() {
	db = await mysql.createConnection({
		host: config.get('masterHost'),
		user: config.get('masterUser'),
		password: config.get('masterPassword'),
		database: config.get('masterDatabase')
	});
	
	if (config.get('deletedLog')) {
		fs.writeFileSync(config.get('deletedLog'), '');
	}
	
	let manifest = await getManifest();
	
	if (!manifest) {
		console.log('Inventory manifest not found');
		return;
	}
	
	CSVCount = manifest.files.length;
	
	let statusInterval = setInterval(printStatus, 1000);
	
	for (let file of manifest.files) {
		CSVCurrent++;
		await processCSV(file.key);
	}
	
	await db.close();
	
	clearInterval(statusInterval);
	
	printStatus();
}

main();
