
-- Grant Key
CREATE TABLE GrantKey (grant_key CHAR(64) NOT NULL, 
	access_key CHAR(64) NOT NULL, 
	PRIMARY KEY(access_key, grant_key));
CREATE TABLE GrantKeyPermission (grant_key CHAR(64) NOT NULL, 
	permission_name ENUM('admin', 'monitor', 'send', 'subscribe') NOT NULL, 
	permission_value BOOLEAN NOT NULL,
	PRIMARY KEY(grant_key, permission_name));
