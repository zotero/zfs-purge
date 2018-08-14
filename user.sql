CREATE USER 'zfs-purge'@'%' IDENTIFIED BY 'password';
GRANT SELECT, DELETE ON `zotero_master`.`storageFiles` TO 'zfs-purge'@'%';
GRANT INSERT, DELETE ON `zotero_master`.`storageFilesDeleted` TO 'zfs-purge'@'%';
GRANT SELECT ON `zotero_master`.`storageFileLibraries` TO 'zfs-purge'@'%';
GRANT SELECT ON `zotero_master`.`storageUploadQueue` TO 'zfs-purge'@'%';
