ALTER TABLE triggers
    ADD COLUMN IF NOT EXISTS function_name TEXT;

UPDATE triggers
SET function_name = target_url
WHERE target_type = 'lambda'
  AND function_name IS NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'triggers_lambda_function_name_chk'
    ) THEN
        ALTER TABLE triggers
            ADD CONSTRAINT triggers_lambda_function_name_chk
            CHECK (
                (target_type = 'lambda' AND function_name IS NOT NULL AND btrim(function_name) <> '')
                OR
                (target_type <> 'lambda')
            );
    END IF;
END $$;
