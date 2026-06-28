-- =============================================================================
-- SYNC_TABLES: Dynamic PL/SQL Procedure for Full Table Comparison & Sync
-- =============================================================================
-- Reads metadata from DBA_TAB_COLUMNS + DBA_CONSTRAINTS at runtime,
-- compares every row between source and target tables,
-- and generates + executes INSERT / UPDATE / DELETE statements dynamically.
--
-- Parameters:
--   p_src_schema   Source schema name (e.g. 'HR')
--   p_src_table    Source table name  (e.g. 'EMPLOYEES')
--   p_tgt_schema   Target schema name (e.g. 'HR_BACKUP')
--   p_tgt_table    Target table name  (e.g. 'EMPLOYEES')
--   p_mode         'EXECUTE' (apply changes) | 'PREVIEW' (print SQL only)
--   p_batch_size   Rows per COMMIT (default 500)
-- =============================================================================

CREATE OR REPLACE PROCEDURE sync_tables (
    p_src_schema  IN VARCHAR2,
    p_src_table   IN VARCHAR2,
    p_tgt_schema  IN VARCHAR2,
    p_tgt_table   IN VARCHAR2,
    p_mode        IN VARCHAR2 DEFAULT 'PREVIEW',   -- 'EXECUTE' or 'PREVIEW'
    p_batch_size  IN NUMBER   DEFAULT 500
)
AS
    -- -------------------------------------------------------------------------
    -- Types
    -- -------------------------------------------------------------------------
    TYPE t_col_rec IS RECORD (
        column_name  VARCHAR2(128),
        data_type    VARCHAR2(128),
        is_pk        VARCHAR2(1)       -- 'Y' / 'N'
    );
    TYPE t_col_tab  IS TABLE OF t_col_rec INDEX BY PLS_INTEGER;
    TYPE t_str_tab  IS TABLE OF VARCHAR2(32767) INDEX BY PLS_INTEGER;
    TYPE t_str_list IS TABLE OF VARCHAR2(128);

    -- -------------------------------------------------------------------------
    -- Variables
    -- -------------------------------------------------------------------------
    v_cols          t_col_tab;
    v_pk_cols       t_str_list := t_str_list();
    v_data_cols     t_str_list := t_str_list();

    v_src_full      VARCHAR2(261);
    v_tgt_full      VARCHAR2(261);

    v_src_cursor    SYS_REFCURSOR;
    v_tgt_cursor    SYS_REFCURSOR;

    -- Dynamic SQL fragments
    v_col_list      VARCHAR2(32767);   -- all column names  (CSV)
    v_pk_join       VARCHAR2(32767);   -- s.PK1=t.PK1 AND ...
    v_data_compare  VARCHAR2(32767);   -- DECODE(s.C,t.C,0,1)+... <> 0
    v_select_src    VARCHAR2(32767);   -- SELECT ... FROM src ORDER BY PKs
    v_select_tgt    VARCHAR2(32767);   -- SELECT ... FROM tgt ORDER BY PKs

    -- Counters
    v_ins_count     PLS_INTEGER := 0;
    v_upd_count     PLS_INTEGER := 0;
    v_del_count     PLS_INTEGER := 0;
    v_batch_ctr     PLS_INTEGER := 0;

    -- Row buffers (fetched as delimited strings for simplicity)
    v_src_row       VARCHAR2(32767);
    v_tgt_row       VARCHAR2(32767);
    v_src_vals      t_str_tab;
    v_tgt_vals      t_str_tab;

    -- DML buffers
    v_dml           VARCHAR2(32767);
    v_set_clause    VARCHAR2(32767);
    v_where_clause  VARCHAR2(32767);
    v_val_clause    VARCHAR2(32767);
    v_insert_cols   VARCHAR2(32767);

    -- Comparison cursor (uses MINUS to find differences)
    v_diff_cur      SYS_REFCURSOR;
    v_diff_type     VARCHAR2(10);      -- 'INSERT','UPDATE','DELETE'
    v_diff_pk       VARCHAR2(32767);   -- serialised PK value(s)

    -- Log table (optional – created if not exists)
    v_log_exists    NUMBER;

    -- -------------------------------------------------------------------------
    -- Helper: log to DBMS_OUTPUT and optionally to SYNC_LOG table
    -- -------------------------------------------------------------------------
    PROCEDURE log_msg (p_msg IN VARCHAR2) IS
    BEGIN
        DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE,'HH24:MI:SS') || ' | ' || p_msg);
    END;

    -- -------------------------------------------------------------------------
    -- Helper: quote a value for SQL depending on data type
    -- -------------------------------------------------------------------------
    FUNCTION quote_val (p_val IN VARCHAR2, p_dtype IN VARCHAR2)
    RETURN VARCHAR2 IS
    BEGIN
        IF p_val IS NULL OR p_val = 'NULL' THEN
            RETURN 'NULL';
        ELSIF p_dtype IN ('NUMBER','FLOAT','INTEGER','BINARY_FLOAT','BINARY_DOUBLE') THEN
            RETURN p_val;
        ELSIF p_dtype IN ('DATE') THEN
            RETURN 'TO_DATE(''' || p_val || ''',''YYYY-MM-DD HH24:MI:SS'')';
        ELSIF p_dtype LIKE 'TIMESTAMP%' THEN
            RETURN 'TO_TIMESTAMP(''' || p_val || ''',''YYYY-MM-DD HH24:MI:SS.FF'')';
        ELSE
            RETURN '''' || REPLACE(p_val, '''', '''''') || '''';
        END IF;
    END;

    -- -------------------------------------------------------------------------
    -- Helper: split delimited string into associative array
    -- -------------------------------------------------------------------------
    PROCEDURE split_row (
        p_str   IN  VARCHAR2,
        p_delim IN  VARCHAR2,
        p_out   OUT t_str_tab
    ) IS
        v_str   VARCHAR2(32767) := p_str;
        v_pos   PLS_INTEGER;
        v_idx   PLS_INTEGER := 1;
    BEGIN
        p_out.DELETE;
        LOOP
            v_pos := INSTR(v_str, p_delim);
            IF v_pos = 0 THEN
                p_out(v_idx) := v_str;
                EXIT;
            END IF;
            p_out(v_idx) := SUBSTR(v_str, 1, v_pos - 1);
            v_str := SUBSTR(v_str, v_pos + LENGTH(p_delim));
            v_idx := v_idx + 1;
        END LOOP;
    END;

    -- -------------------------------------------------------------------------
    -- Helper: ensure SYNC_LOG table exists
    -- -------------------------------------------------------------------------
    PROCEDURE ensure_log_table IS
    BEGIN
        SELECT COUNT(*) INTO v_log_exists
        FROM   user_tables
        WHERE  table_name = 'SYNC_LOG';

        IF v_log_exists = 0 THEN
            EXECUTE IMMEDIATE '
                CREATE TABLE sync_log (
                    log_id       NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    run_time     DATE          DEFAULT SYSDATE,
                    src_table    VARCHAR2(261),
                    tgt_table    VARCHAR2(261),
                    operation    VARCHAR2(10),
                    pk_values    VARCHAR2(4000),
                    status       VARCHAR2(10),
                    error_msg    VARCHAR2(4000)
                )';
            log_msg('Created SYNC_LOG table.');
        END IF;
    END;

BEGIN
    -- =========================================================================
    -- 0. Initialise
    -- =========================================================================
    DBMS_OUTPUT.ENABLE(1000000);
    v_src_full := UPPER(p_src_schema) || '.' || UPPER(p_src_table);
    v_tgt_full := UPPER(p_tgt_schema) || '.' || UPPER(p_tgt_table);

    log_msg('=== SYNC_TABLES START ===');
    log_msg('Source : ' || v_src_full);
    log_msg('Target : ' || v_tgt_full);
    log_msg('Mode   : ' || p_mode);

    IF p_mode NOT IN ('EXECUTE','PREVIEW') THEN
        RAISE_APPLICATION_ERROR(-20001, 'p_mode must be EXECUTE or PREVIEW');
    END IF;

    IF p_mode = 'EXECUTE' THEN
        ensure_log_table;
    END IF;

    -- =========================================================================
    -- 1. Load column metadata from DBA_TAB_COLUMNS + DBA_CONSTRAINTS
    -- =========================================================================
    log_msg('Step 1: Reading DBA_TAB_COLUMNS ...');

    DECLARE
        CURSOR c_cols IS
            SELECT  tc.column_name,
                    tc.data_type,
                    CASE WHEN pk.column_name IS NOT NULL THEN 'Y' ELSE 'N' END AS is_pk
            FROM    dba_tab_columns tc
            LEFT JOIN (
                SELECT  cc.column_name
                FROM    dba_cons_columns  cc
                JOIN    dba_constraints   c
                  ON    c.constraint_name = cc.constraint_name
                  AND   c.owner           = cc.owner
                WHERE   c.constraint_type = 'P'
                  AND   c.owner           = UPPER(p_src_schema)
                  AND   cc.table_name     = UPPER(p_src_table)
            ) pk ON pk.column_name = tc.column_name
            WHERE   tc.owner       = UPPER(p_src_schema)
              AND   tc.table_name  = UPPER(p_src_table)
            ORDER BY tc.column_id;
        v_idx PLS_INTEGER := 1;
    BEGIN
        FOR r IN c_cols LOOP
            v_cols(v_idx).column_name := r.column_name;
            v_cols(v_idx).data_type   := r.data_type;
            v_cols(v_idx).is_pk       := r.is_pk;

            IF r.is_pk = 'Y' THEN
                v_pk_cols.EXTEND;
                v_pk_cols(v_pk_cols.LAST) := r.column_name;
            ELSE
                v_data_cols.EXTEND;
                v_data_cols(v_data_cols.LAST) := r.column_name;
            END IF;
            v_idx := v_idx + 1;
        END LOOP;
    END;

    IF v_cols.COUNT = 0 THEN
        RAISE_APPLICATION_ERROR(-20002,
            'No columns found for ' || v_src_full ||
            '. Check schema/table name and privileges on DBA_TAB_COLUMNS.');
    END IF;

    IF v_pk_cols.COUNT = 0 THEN
        RAISE_APPLICATION_ERROR(-20003,
            'No primary key found on ' || v_src_full ||
            '. A PK is required for row-level comparison.');
    END IF;

    log_msg('  Columns  : ' || v_cols.COUNT);
    log_msg('  PK cols  : ' || v_pk_cols.COUNT);
    log_msg('  Data cols: ' || v_data_cols.COUNT);

    -- =========================================================================
    -- 2. Build reusable SQL fragments
    -- =========================================================================
    log_msg('Step 2: Building SQL fragments ...');

    -- 2a. Column list  (col1, col2, ...)
    FOR i IN 1 .. v_cols.COUNT LOOP
        v_col_list := v_col_list
                   || CASE WHEN i > 1 THEN ', ' END
                   || v_cols(i).column_name;
    END LOOP;

    -- 2b. PK join predicate  s.PK1 = t.PK1 AND ...
    FOR i IN 1 .. v_pk_cols.COUNT LOOP
        v_pk_join := v_pk_join
                  || CASE WHEN i > 1 THEN ' AND ' END
                  || 's.' || v_pk_cols(i) || ' = t.' || v_pk_cols(i);
    END LOOP;

    -- 2c. Data-column change detector
    --     SUM(CASE WHEN s.C != t.C OR (s.C IS NULL) != (t.C IS NULL) THEN 1 ELSE 0 END)
    IF v_data_cols.COUNT > 0 THEN
        FOR i IN 1 .. v_data_cols.COUNT LOOP
            v_data_compare := v_data_compare
                           || CASE WHEN i > 1 THEN ' + ' END
                           || 'CASE WHEN (s.' || v_data_cols(i)
                           || ' <> t.' || v_data_cols(i)
                           || ' OR (s.' || v_data_cols(i) || ' IS NULL) <> (t.' || v_data_cols(i)
                           || ' IS NULL)) THEN 1 ELSE 0 END';
        END LOOP;
    END IF;

    -- =========================================================================
    -- 3. Identify differences via set-based SQL
    --    Uses three MINUS / NOT EXISTS queries combined with UNION ALL
    -- =========================================================================
    log_msg('Step 3: Identifying differences (INSERT/UPDATE/DELETE) ...');

    DECLARE
        v_diff_sql  VARCHAR2(32767);
        v_pk_src    VARCHAR2(4000);
        v_pk_tgt    VARCHAR2(4000);
        v_pk_sel_s  VARCHAR2(4000);
        v_pk_sel_t  VARCHAR2(4000);
        v_op        VARCHAR2(10);
        v_pk_val    VARCHAR2(4000);
    BEGIN
        -- Serialise PK for logging (|| all PK cols into one string)
        FOR i IN 1 .. v_pk_cols.COUNT LOOP
            v_pk_sel_s := v_pk_sel_s
                       || CASE WHEN i > 1 THEN ' || ''|'' || ' END
                       || 'TO_CHAR(s.' || v_pk_cols(i) || ')';
            v_pk_sel_t := v_pk_sel_t
                       || CASE WHEN i > 1 THEN ' || ''|'' || ' END
                       || 'TO_CHAR(t.' || v_pk_cols(i) || ')';
        END LOOP;

        -- UNION ALL of three change sets
        v_diff_sql :=
            -- Rows in SOURCE not in TARGET  → INSERT
            'SELECT ''INSERT'' AS op, ' || v_pk_sel_s || ' AS pk_val' ||
            ' FROM ' || v_src_full || ' s' ||
            ' WHERE NOT EXISTS (SELECT 1 FROM ' || v_tgt_full || ' t WHERE ' || v_pk_join || ')' ||

            ' UNION ALL ' ||

            -- Rows in TARGET not in SOURCE  → DELETE
            'SELECT ''DELETE'', ' || v_pk_sel_t ||
            ' FROM ' || v_tgt_full || ' t' ||
            ' WHERE NOT EXISTS (SELECT 1 FROM ' || v_src_full || ' s WHERE ' || v_pk_join || ')';

        -- Rows in both but data changed    → UPDATE (only if data cols exist)
        IF v_data_cols.COUNT > 0 THEN
            v_diff_sql := v_diff_sql ||
                ' UNION ALL ' ||
                'SELECT ''UPDATE'', ' || v_pk_sel_s ||
                ' FROM ' || v_src_full || ' s' ||
                ' JOIN '  || v_tgt_full || ' t ON (' || v_pk_join || ')' ||
                ' WHERE (' || v_data_compare || ') > 0';
        END IF;

        OPEN v_diff_cur FOR v_diff_sql;

        LOOP
            FETCH v_diff_cur INTO v_op, v_pk_val;
            EXIT WHEN v_diff_cur%NOTFOUND;

            -- ---------------------------------------------------------------
            -- 3a. For each difference, fetch full source/target row and build DML
            -- ---------------------------------------------------------------
            DECLARE
                v_fetch_sql  VARCHAR2(32767);
                v_cur        SYS_REFCURSOR;
                v_row_vals   t_str_tab;
                v_concat_row VARCHAR2(32767);
                v_col_csv    VARCHAR2(32767);
                v_set        VARCHAR2(32767);
                v_where      VARCHAR2(32767);
                v_ins_cols   VARCHAR2(32767);
                v_ins_vals   VARCHAR2(32767);

                -- Build a delimited string of all column values for one row
                FUNCTION row_to_str (p_schema VARCHAR2, p_table VARCHAR2, p_pk_vals VARCHAR2)
                RETURN VARCHAR2 IS
                    v_sel    VARCHAR2(32767);
                    v_result VARCHAR2(32767);
                    v_cur2   SYS_REFCURSOR;
                    -- Rebuild WHERE from PK value(s) using position
                    v_wh     VARCHAR2(4000);
                    v_pks    t_str_tab;
                    v_delim  VARCHAR2(1) := '|';
                BEGIN
                    -- Reconstruct PK WHERE clause
                    -- (single PK: simple; composite: split on '|')
                    split_row(p_pk_vals, v_delim, v_pks);
                    FOR i IN 1 .. v_pk_cols.COUNT LOOP
                        v_wh := v_wh
                             || CASE WHEN i > 1 THEN ' AND ' END
                             || v_pk_cols(i) || ' = '
                             || quote_val(v_pks(i),
                                    (SELECT data_type FROM dba_tab_columns
                                     WHERE owner=UPPER(p_schema)
                                       AND table_name=UPPER(p_table)
                                       AND column_name=v_pk_cols(i)));
                    END LOOP;

                    -- Build SELECT that concatenates all cols with chr(1) delimiter
                    v_sel := 'SELECT ';
                    FOR i IN 1 .. v_cols.COUNT LOOP
                        v_sel := v_sel
                               || CASE WHEN i > 1 THEN ' || CHR(1) || ' END
                               || 'NVL(TO_CHAR(' || v_cols(i).column_name || '),''NULL'')';
                    END LOOP;
                    v_sel := v_sel || ' FROM ' || p_schema || '.' || p_table
                          || ' WHERE ' || v_wh;

                    OPEN v_cur2 FOR v_sel;
                    FETCH v_cur2 INTO v_result;
                    CLOSE v_cur2;
                    RETURN v_result;
                END;

            BEGIN
                CASE v_op
                -- ===========================================================
                WHEN 'INSERT' THEN
                -- ===========================================================
                    v_concat_row := row_to_str(p_src_schema, p_src_table, v_pk_val);
                    split_row(v_concat_row, CHR(1), v_row_vals);

                    v_ins_cols := v_col_list;
                    v_ins_vals := '';
                    FOR i IN 1 .. v_cols.COUNT LOOP
                        v_ins_vals := v_ins_vals
                                   || CASE WHEN i > 1 THEN ', ' END
                                   || quote_val(v_row_vals(i), v_cols(i).data_type);
                    END LOOP;

                    v_dml := 'INSERT INTO ' || v_tgt_full
                          || ' (' || v_ins_cols || ')'
                          || ' VALUES (' || v_ins_vals || ')';

                    v_ins_count := v_ins_count + 1;

                -- ===========================================================
                WHEN 'UPDATE' THEN
                -- ===========================================================
                    DECLARE
                        v_src_concat VARCHAR2(32767);
                        v_src_split  t_str_tab;
                        v_tgt_concat VARCHAR2(32767);
                        v_tgt_split  t_str_tab;
                    BEGIN
                        v_src_concat := row_to_str(p_src_schema, p_src_table, v_pk_val);
                        v_tgt_concat := row_to_str(p_tgt_schema, p_tgt_table, v_pk_val);
                        split_row(v_src_concat, CHR(1), v_src_split);
                        split_row(v_tgt_concat, CHR(1), v_tgt_split);

                        v_set   := '';
                        v_where := '';

                        -- SET clause: only changed data columns
                        FOR i IN 1 .. v_cols.COUNT LOOP
                            IF v_cols(i).is_pk = 'N' THEN
                                IF NVL(v_src_split(i),'NULL') != NVL(v_tgt_split(i),'NULL') THEN
                                    v_set := v_set
                                          || CASE WHEN LENGTH(v_set) > 0 THEN ', ' END
                                          || v_cols(i).column_name || ' = '
                                          || quote_val(v_src_split(i), v_cols(i).data_type);
                                END IF;
                            END IF;
                        END LOOP;

                        -- WHERE clause: PK columns
                        FOR i IN 1 .. v_cols.COUNT LOOP
                            IF v_cols(i).is_pk = 'Y' THEN
                                v_where := v_where
                                        || CASE WHEN LENGTH(v_where) > 0 THEN ' AND ' END
                                        || v_cols(i).column_name || ' = '
                                        || quote_val(v_src_split(i), v_cols(i).data_type);
                            END IF;
                        END LOOP;

                        v_dml := 'UPDATE ' || v_tgt_full
                              || ' SET ' || v_set
                              || ' WHERE ' || v_where;
                        v_upd_count := v_upd_count + 1;
                    END;

                -- ===========================================================
                WHEN 'DELETE' THEN
                -- ===========================================================
                    DECLARE
                        v_tgt_concat VARCHAR2(32767);
                        v_tgt_split  t_str_tab;
                    BEGIN
                        v_tgt_concat := row_to_str(p_tgt_schema, p_tgt_table, v_pk_val);
                        split_row(v_tgt_concat, CHR(1), v_tgt_split);

                        v_where := '';
                        FOR i IN 1 .. v_cols.COUNT LOOP
                            IF v_cols(i).is_pk = 'Y' THEN
                                v_where := v_where
                                        || CASE WHEN LENGTH(v_where) > 0 THEN ' AND ' END
                                        || v_cols(i).column_name || ' = '
                                        || quote_val(v_tgt_split(i), v_cols(i).data_type);
                            END IF;
                        END LOOP;

                        v_dml := 'DELETE FROM ' || v_tgt_full || ' WHERE ' || v_where;
                        v_del_count := v_del_count + 1;
                    END;
                END CASE;

                -- ---------------------------------------------------------------
                -- 4. Execute or print the DML
                -- ---------------------------------------------------------------
                IF p_mode = 'PREVIEW' THEN
                    DBMS_OUTPUT.PUT_LINE('-- [' || v_op || '] pk=' || v_pk_val);
                    DBMS_OUTPUT.PUT_LINE(v_dml || ';');
                    DBMS_OUTPUT.PUT_LINE('');
                ELSE
                    BEGIN
                        EXECUTE IMMEDIATE v_dml;

                        -- Log success
                        EXECUTE IMMEDIATE
                            'INSERT INTO sync_log (src_table,tgt_table,operation,pk_values,status)
                             VALUES (:1,:2,:3,:4,''OK'')'
                        USING v_src_full, v_tgt_full, v_op, v_pk_val;

                        -- Batch commit
                        v_batch_ctr := v_batch_ctr + 1;
                        IF v_batch_ctr >= p_batch_size THEN
                            COMMIT;
                            v_batch_ctr := 0;
                            log_msg('  Batch commit after '
                                  || p_batch_size || ' rows.');
                        END IF;

                    EXCEPTION
                        WHEN OTHERS THEN
                            -- Log error row and continue
                            EXECUTE IMMEDIATE
                                'INSERT INTO sync_log
                                    (src_table,tgt_table,operation,pk_values,status,error_msg)
                                 VALUES (:1,:2,:3,:4,''ERROR'',:5)'
                            USING v_src_full, v_tgt_full, v_op, v_pk_val,
                                  SUBSTR(SQLERRM,1,4000);
                            log_msg('  ERROR on ' || v_op || ' pk=' || v_pk_val
                                  || ' -> ' || SQLERRM);
                    END;
                END IF;

            END; -- inner DECLARE
        END LOOP;

        CLOSE v_diff_cur;
    END; -- Step 3 DECLARE

    -- =========================================================================
    -- 5. Final commit and summary
    -- =========================================================================
    IF p_mode = 'EXECUTE' AND v_batch_ctr > 0 THEN
        COMMIT;
    END IF;

    log_msg('');
    log_msg('=== SYNC SUMMARY ===');
    log_msg('  Inserts : ' || v_ins_count);
    log_msg('  Updates : ' || v_upd_count);
    log_msg('  Deletes : ' || v_del_count);
    log_msg('  Total   : ' || (v_ins_count + v_upd_count + v_del_count));
    IF p_mode = 'EXECUTE' THEN
        log_msg('  Status  : Changes applied and committed.');
        log_msg('  Log     : SELECT * FROM sync_log ORDER BY log_id DESC;');
    ELSE
        log_msg('  Status  : PREVIEW only – no changes applied.');
    END IF;
    log_msg('=== SYNC_TABLES END ===');

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        log_msg('FATAL ERROR: ' || SQLERRM);
        log_msg(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
        RAISE;
END sync_tables;
/

-- =============================================================================
-- USAGE EXAMPLES
-- =============================================================================

-- 1. Preview (print SQL without applying anything)
--    EXECUTE IMMEDIATE is not run; you see the statements via DBMS_OUTPUT.
SET SERVEROUTPUT ON SIZE UNLIMITED;
BEGIN
    sync_tables(
        p_src_schema => 'HR',
        p_src_table  => 'EMPLOYEES',
        p_tgt_schema => 'HR_BACKUP',
        p_tgt_table  => 'EMPLOYEES',
        p_mode       => 'PREVIEW'
    );
END;
/

-- 2. Execute sync (applies INSERT/UPDATE/DELETE with batch commits of 500)
BEGIN
    sync_tables(
        p_src_schema => 'HR',
        p_src_table  => 'EMPLOYEES',
        p_tgt_schema => 'HR_BACKUP',
        p_tgt_table  => 'EMPLOYEES',
        p_mode       => 'EXECUTE',
        p_batch_size => 500
    );
END;
/

-- 3. Review the log after execution
SELECT operation, pk_values, status, error_msg, run_time
FROM   sync_log
WHERE  src_table = 'HR.EMPLOYEES'
ORDER  BY log_id DESC;
