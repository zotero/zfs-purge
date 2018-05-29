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

const S3ZFS = new AWS.S3(config.get('S3ZFS'));
const S3Inventory = new AWS.S3(config.get('S3Inventory'));

let db = null;

let numDeleted = 0;

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

async function processCSV(key) {
	let result = await S3Inventory.getObject({Key: key}).promise();
	let csv = zlib.gunzipSync(result.Body);
	let rows = parse(csv);
	
	let group = [];
	for (let row of rows) {
		let key = row[1];
		let hash = key.split('/')[0];
		if (!/^[a-z0-9]{32}$/.test(hash)) continue;
		
		if (group.length && group[0].split('/')[0] !== hash) {
			await purgeGroup(group);
			group = [];
		}
		
		group.push(key);
	}
	
	if (group.length) {
		await purgeGroup(group);
	}
}

async function S3Delete(keys) {
	let partKeys = keys.splice(0, 1000);
	
	numDeleted += partKeys.length;
	console.log('Deleting (' + numDeleted + ') ' + partKeys.join(','));
	
	let objects = partKeys.map(key => {
		Key: key
	});
	
	let params = {
		Delete: {
			Objects: objects
		}
	};
	
	// await S3ZFS.deleteObjects(params).promise();
	
	if (keys.length) {
		await S3Delete(keys);
	}
}

// Is it possible that a record in storage items exists, but file in S3 no. Previous deletes?
async function purgeGroup(group) {
	let hash = group[0].split('/')[0];
	
	await db.beginTransaction();
	
	// Check if the file is still in use and lock all storageFiles rows containing the hash
	let [res] = await db.execute(`
		SELECT MAX(sfl.libraryID) AS libraryID
		FROM storageFiles sf
		LEFT JOIN storageFileLibraries sfl ON (sfl.storageFileID = sf.storageFileID)
		WHERE hash = ?
		FOR UPDATE`,
		[hash]
	);
	
	let deleteKeys = [];
	// If none of libraries have the file, delete all keys from S3
	if (res[0].libraryID === null) {
		deleteKeys = group;
	}
	// Otherwise just delete the redundant copies
	else if (group.length >= 2) {
		// Only keep the shortest key. I.e. 'bucket/hash' if exists,
		// otherwise just 'bucket/hash/shortest_filename'
		group.sort((a, b) => a.length - b.length);
		deleteKeys = group.slice(1);
	}
	
	if (deleteKeys.length) {
		await S3Delete(deleteKeys);
		
		// Delete all storageFile rows that are no longer used in storageFileLibraries
		await db.execute(`
			DELETE sf FROM storageFiles sf
			LEFT JOIN storageFileLibraries sfl ON (sfl.storageFileID = sf.storageFileID)
			WHERE hash = ? AND sfl.libraryID IS NULL`,
			[hash]
		);
	}
	
	await db.commit();
}

async function main() {
	db = await mysql.createConnection({
		host: config.get('masterHost'),
		user: config.get('masterUser'),
		password: config.get('masterPassword'),
		database: config.get('masterDatabase')
	});
	
	let manifest = await getManifest();
	
	console.log('CSV files in manifest: ' + manifest.files.length);
	
	for (let i = 0; i < manifest.files.length; i++) {
		let file = manifest.files[i];
		console.log('Processing ' + (i + 1) + '/' + manifest.files.length + ': ' + file.key);
		await processCSV(file.key);
	}
}

main();
