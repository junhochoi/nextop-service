
CREATE UNIQUE INDEX Overlord_UniqueLocalKey ON Overlord (local_key);
DROP INDEX Overlord_ByAccessKey ON Overlord;
CREATE INDEX Overlord_ByAccessKey ON Overlord (access_key, public_host, port, local_key);
