DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_enum e
        JOIN pg_type t ON t.oid = e.enumtypid
        WHERE t.typname = 'trigger_target_type' AND e.enumlabel = 'lambda'
    ) THEN
        ALTER TYPE trigger_target_type ADD VALUE 'lambda';
    END IF;
END $$;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_enum e
        JOIN pg_type t ON t.oid = e.enumtypid
        WHERE t.typname = 'trigger_target_type' AND e.enumlabel = 'lambda'
    ) THEN
        ALTER TYPE trigger_target_type ADD VALUE 'lambda';
    END IF;
END $$;
