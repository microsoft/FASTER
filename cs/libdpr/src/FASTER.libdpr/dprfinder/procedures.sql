CREATE TABLE dpr (
                     workerId VARCHAR(255),
                     persistedVersion BIGINT,
                     safeVersion BIGINT,
                     lastUpdated DATETIME,
                     PRIMARY KEY (workerId)
)

CREATE TABLE worldLines (
                            workerId VARCHAR(255),
                            worldLine BIGINT
                                PRIMARY KEY (workerId)
)

    INSERT INTO worldLines VALUES ('dprManager', 0)

CREATE TABLE deps (
                      fromWorker VARCHAR(255),
                      fromVersion BIGINT,
                      toWorker VARCHAR(255),
                      toVersion BIGINT,
)

CREATE CLUSTERED INDEX persistentWorkerVersions ON deps(fromWorker, fromVersion)

CREATE FUNCTION systemRecovering(@dprManagerWorldLine BIGINT) RETURNS BIT
AS BEGIN
    RETURN IIF (EXISTS(SELECT * FROM worldLines WHERE worldLine < @dprManagerWorldLine), 1, 0)
END

CREATE PROCEDURE drainDeps
    AS BEGIN
BEGIN TRANSACTION
SELECT * FROM deps
DELETE FROM deps
    COMMIT
END

CREATE PROCEDURE upsertVersion @worker VARCHAR(255), @version BIGINT, @oegVersion BIGINT
AS BEGIN
BEGIN TRANSACTION
        DECLARE @now DATETIME
SELECT @now = CURRENT_TIMESTAMP
UPDATE dpr SET persistedVersion = @version, safeVersion = @oegVersion, lastUpdated = @now WHERE workerId = @worker
    IF (@@ROWCOUNT = 0)
INSERT INTO dpr VALUES(@worker, @version, @oegVersion, @now)

DECLARE @systemWorldLine BIGINT
SELECT @systemWorldLine = worldLine FROM worldLines WHERE workerId = 'dprManager'
    IF (dbo.systemRecovering(@systemWorldLine) = 1)
BEGIN
ROLLBACK
    RETURN
END
COMMIT
END

CREATE PROCEDURE getTableUpdatesV1
    AS BEGIN
BEGIN TRANSACTION
        DECLARE @systemWorldLine BIGINT
SELECT @systemWorldLine = worldLine FROM worldLines WHERE workerId = 'dprManager'
SELECT MAX(persistedVersion), MIN(persistedVersion), @systemWorldLine FROM dpr
    COMMIT
END

CREATE PROCEDURE getTableUpdatesV2
    AS BEGIN
BEGIN TRANSACTION
    DECLARE @systemWorldLine BIGINT
SELECT @systemWorldLine = worldLine FROM worldLines WHERE workerId = 'dprManager'
DECLARE @min BIGINT, @max BIGINT
SELECT @min = MIN(persistedVersion), @max = MAX(persistedVersion) FROM dpr
SELECT workerId, safeVersion FROM dpr WHERE safeVersion > @min
SELECT @max, @min, @systemWorldLine
    COMMIT
END

CREATE PROCEDURE getTableUpdatesV3
    AS BEGIN
BEGIN TRANSACTION
    DECLARE @systemWorldLine BIGINT
SELECT @systemWorldLine = worldLine FROM worldLines WHERE workerId = 'dprManager'
SELECT CURRENT_TIMESTAMP, @systemWorldLine
SELECT workerId, safeVersion FROM dpr
    COMMIT
END

CREATE PROCEDURE cleanup
    AS BEGIN
DELETE FROM dpr
DELETE FROM deps
DELETE FROM worldLines WHERE workerId <> 'dprManager'
UPDATE worldLines SET worldLine = 0
END

CREATE PROCEDURE setSystemWorldLine @worldLine BIGINT
AS
UPDATE worldLines SET worldLine = @worldLine WHERE workerId = 'dprManager'

CREATE PROCEDURE reportRecoveryV1 @workerId VARCHAR(255), @worldLine BIGINT, @survivingVersion BIGINT
AS BEGIN
UPDATE dpr SET persistedVersion = @survivingVersion WHERE workerId = @workerId
UPDATE worldLines SET worldLine = @worldLine WHERE workerId = @workerId
END

CREATE PROCEDURE reportRecoveryV3 @workerId VARCHAR(255), @worldLine BIGINT, @survivingVersion BIGINT
AS BEGIN
DELETE FROM deps WHERE fromWorker = @workerId AND fromVersion > @survivingVersion
UPDATE worldLines SET worldLine = @worldLine WHERE workerId = @workerId
END

CREATE FUNCTION workerVersionPersistent(@worker VARCHAR(255), @version BIGINT) RETURNS BIT
AS BEGIN
    RETURN IIF (EXISTS (SELECT * FROM dpr WHERE @worker = workerId AND safeVersion >= @version)
                    OR EXISTS(SELECT * FROM deps WHERE fromWorker = @worker AND fromVersion = @version), 1, 0)
END

--- returns the set of all uncommitted dependency of the given worker-version if it can be committed, of empty otherwise
CREATE FUNCTION dprDependencySet (@worker VARCHAR(255), @version BIGINT) RETURNS @visited TABLE(cWorker VARCHAR(255), cVersion BIGINT, PRIMARY KEY (cWorker, cVersion))
AS BEGIN
    --- essentially a bfs operation to build the dependency set
    DECLARE @previousSafeVersion BIGINT
SELECT @previousSafeVersion = safeVersion FROM dpr WHERE workerId = @worker
                                                         --- no need to try advance if already persistent
    IF (@version <= @previousSafeVersion)
        RETURN

DECLARE @frontier TABLE (worker VARCHAR(255), version BIGINT, PRIMARY KEY (worker, version))
    INSERT INTO @visited VALUES (@worker, @version)
    INSERT INTO @frontier VALUES (@worker, @version)

    WHILE @@ROWCOUNT <> 0
BEGIN
            DECLARE @newFrontier TABLE (worker VARCHAR(255), version BIGINT, PRIMARY KEY (worker, version))
            INSERT INTO @newFrontier
SELECT DISTINCT toWorker, toVersion
FROM
    @frontier JOIN deps ON worker = fromWorker AND  version = fromVersion
WHERE
        toVersion > (SELECT safeVersion FROM dpr WHERE workerId = toWorker) AND
    NOT EXISTS (SELECT * FROM @visited WHERE cWorker = toWorker AND cVersion = toVersion)

  --- Only persistent versions show up in the deps table, if one of the dependency sets contains a non-persistent
  --- version, the version is not recoverable. We can discard.
    IF EXISTS (SELECT * FROM @newFrontier WHERE dbo.workerVersionPersistent(worker, version) = 0)
BEGIN
DELETE FROM @visited
                RETURN
END

DELETE FROM @frontier
    INSERT INTO @frontier SELECT * FROM @newFrontier
    INSERT INTO @visited SELECT * FROM @newFrontier EXCEPT (SELECT worker, version FROM @newFrontier JOIN @visited ON worker = cWorker AND version = cVersion)
DELETE FROM @newFrontier
END
    --- if we haven't return by now, all versions in the dependency set are persistent, can advance dpr past this
    ---  version
END

CREATE PROCEDURE tryAdvanceAllCpr
    AS BEGIN
BEGIN TRANSACTION
        DECLARE depsCursor CURSOR
            LOCAL FORWARD_ONLY
            FOR
SELECT fromWorker, MIN(fromVersion) FROM deps GROUP BY fromWorker

DECLARE @worker VARCHAR(255)
        DECLARE @version BIGINT

        OPEN depsCursor
        FETCH NEXT FROM depsCursor INTO @worker, @version
        WHILE @@FETCH_STATUS = 0
BEGIN
                DECLARE @updates TABLE(worker VARCHAR(255), version BIGINT, PRIMARY KEY (worker, version))
                INSERT INTO @updates SELECT cWorker, cVersion FROM dprDependencySet(@worker, @version)
DELETE deps FROM deps JOIN @updates ON fromWorker = worker AND fromVersion = version

UPDATE
    dpr
SET
    safeVersion = updates.m
    FROM 
                    (SELECT worker,  MAX(version) AS m FROM @updates GROUP BY worker) AS updates
WHERE dpr.workerId = updates.worker

    FETCH NEXT FROM depsCursor INTO @worker, @version
END
CLOSE depsCursor
    DEALLOCATE depsCursor

DECLARE @systemWorldLine BIGINT
SELECT @systemWorldLine = worldLine FROM worldLines WHERE workerId = 'dprManager'
    IF (dbo.systemRecovering(@systemWorldLine) = 1)
BEGIN
ROLLBACK
    RETURN
END
COMMIT
END