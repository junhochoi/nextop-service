
-- store 256-bit UUIDs as hex strings CHAR(64)


-- Overlord
-- this stores assigned and unassigned overlord authorities
-- e.g. scale out of the capacity of the system adds entries here
CREATE TABLE Overlord (public_host VARCHAR(32) NOT NULL, 
	port SMALLINT UNSIGNED,
	cloud ENUM('aws', 'gce', 'ma') NOT NULL,
	access_key CHAR(64) NULL DEFAULT NULL, 
	local_key CHAR(64) NULL DEFAULT NULL,
	PRIMARY KEY (public_host, port));
CREATE INDEX Overlord_ByAccessKey ON Overlord (access_key, public_host, port);

CREATE TABLE OverlordStatus (local_key CHAR(64) NULL,
	git_commit_hash CHAR(60) NULL,
	deep_md5 CHAR(32) NULL,
	monitor_up BOOLEAN DEFAULT false,
	PRIMARY KEY (local_key));
