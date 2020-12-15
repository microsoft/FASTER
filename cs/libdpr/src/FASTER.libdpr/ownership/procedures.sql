﻿﻿CREATE TABLE ownership (
    bucket VARCHAR,
    owner BIGINT
    PRIMARY KEY (bucket)
)

CREATE PROCEDURE findOwner @bucket VARCHAR
AS
    SELECT owner FROM ownership WHERE bucket = @bucket;

CREATE PROCEDURE remove @bucket VARCHAR
AS
    DELETE  FROM ownership WHERE bucket = @bucket;

CREATE PROCEDURE obtainOwnership @bucket VARCHAR, @newOwner BIGINT, @expectedOwner BIGINT
AS BEGIN
    BEGIN TRANSACTION
    DECLARE @oldOwner BIGINT
    SELECT @oldOwner = owner FROM ownership WHERE bucket = @bucket
    IF (@@ROWCOUNT = 0) BEGIN
        IF (@expectedOwner = -1) BEGIN
            INSERT INTO ownership VALUES (@bucket, @newOwner)
            COMMIT
            SELECT @newOwner
            RETURN
        END
        COMMIT
        SELECT -1
        RETURN
    END
    
    IF (@oldOwner <> @expectedOwner) BEGIN
        COMMIT
        SELECT @oldOwner
        RETURN
    END
    
    UPDATE ownership SET owner = @newOwner WHERE bucket = @bucket
    COMMIT
    SELECT @newOwner
    RETURN
END