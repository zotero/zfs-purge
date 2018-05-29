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

const config = require('config');
const AWS = require('aws-sdk');

const S3ZFS = new AWS.S3(config.get('S3ZFS'));
const S3Inventory = new AWS.S3(config.get('S3Inventory'));

async function main() {
	let params = {
		Bucket: config.get('S3ZFS').params.Bucket,
		Id: config.get('inventoryId'),
		InventoryConfiguration: {
			Schedule: {
				Frequency: 'Weekly'
			},
			IsEnabled: true,
			Destination: {
				S3BucketDestination: {
					Bucket: 'arn:aws:s3:::' + config.get('S3Inventory').params.Bucket,
					Format: 'CSV'
				}
			},
			OptionalFields: ['Size', 'LastModifiedDate', 'ETag'],
			IncludedObjectVersions: 'Current',
			Id: config.get('inventoryId')
		}
	};
	
	let res = await S3Inventory.putBucketInventoryConfiguration(params).promise();
}

main();
