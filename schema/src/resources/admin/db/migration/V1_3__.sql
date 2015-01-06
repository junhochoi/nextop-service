

ALTER TABLE OverlordStatus DROP COLUMN git_commit_hash;
ALTER TABLE OverlordStatus DROP COLUMN deep_md5;
ALTER TABLE OverlordStatus ADD COLUMN package_tag VARCHAR(250) NULL;
ALTER TABLE OverlordStatus ADD COLUMN terminating BOOLEAN DEFAULT false;

CREATE TABLE AccessKeyStatus (access_key CHAR(64) NOT NULL,
	package_tag VARCHAR(250) NULL,
	terminating BOOLEAN DEFAULT false,
	PRIMARY KEY(access_key));
