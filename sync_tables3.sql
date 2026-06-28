-- =============================================================================
-- SYNC_TABLES: Dynamic PL/SQL Procedure – Source accessed via DB Link
-- =============================================================================
-- The SOURCE table lives in a remote database accessed through an Oracle
-- DB Link.  All metadata queries (DBA_TAB_COLUMNS, DBA_CONSTRAINTS) and
-- all data queries against the source are suffixed with @<dblink>.
-- The TARGET table is local; all DML is applied locally.
--
-- Parameters:
--   p_src_schema   Remote source schema          (e.g. 'HR')
--   p_src_table    Remote source table           (e.g. 'EMPLOYEES')
--   p_db_link      DB Link name                  (e.g. 'PROD_LINK')
--   p_tgt_schema   Local target schema           (e.g. 'HR_BACKUP')
--   p_tgt_table    Local target table            (e.g. 'EMPLOYEES')
--   p_mode         'EXECUTE' (apply) | 'PREVIEW' (print SQL only)
--   p_batch_size   Rows per COMMIT in EXECUTE mode (default 500)
--
-- Prerequisites:
--   GRANT SELECT ON DBA_TAB_COLUMNS  TO <user> (or use ALL_TAB_COLUMNS@link)
--   GRANT SELECT ON DBA_CONSTRAINTS  TO <user>
--   GRANT SELECT ON DBA_CONS_COLUMNS TO <user>
--   The DB Link must be accessible from the local schema.
-- =============================================================================

CREATE OR REPLACE PROCEDURE sync_tables (
    p_src_schema  IN VARCHAR2,
    p_src_table   IN VARCHAR2,
    p_db_link     IN VARCHAR2,           -- <<< NEW: remote DB link name
    p_tgt_schema  IN VARCHAR2,
    p_tgt_table   IN VARCHAR2,
    p_mode        IN VARCHAR2 DEFAULT 'PREVIEW',
    p_batch_size  IN NUMBER   DEFAULT 500
)
AS
    -- -------------------------------------------------------------------------
    -- Type definitions
    -- -------------------------------------------------------------------------
    TYPE t_col_rec IS RECORD (
        column_name  VARCHAR2(128),
        data_type    VARCHAR2(128),
        is_pk        VARCHAR2(1)
    );
    TYPE t_col_tab  IS TABLE OF t_col_rec INDEX BY PLS_INTEGER;
    TYPE t_str_tab  IS TABLE OF VARCHAR2(32767) INDEX BY PLS_INTEGER;
    TYPE t_str_list IS TABLE OF VARCHAR2(128);

    -- -------------------------------------------------------------------------
    -- State variables
    -- -------------------------------------------------------------------------
    v_cols          t_col_tab;
    v_pk_cols       t_str_list := t_str_list();
    v_data_cols     t_str_list := t_str_list();

    v_db_link       VARCHAR2(128);       -- sanitised link name (no @)
    v_at_link       VARCHAR2(130);       -- '@PROD_LINK'  – appended to remote refs
    v_src_full      VARCHAR2(400);       -- SCHEMA.TABLE@LINK
    v_tgt_full      VARCHAR2(261);       -- SCHEMA.TABLE  (local)

    v_col_list      VARCHAR2(32767);
    v_pk_join       VARCHAR2(32767);
    v_data_compare  VARCHAR2(32767);

    v_ins_count     PLS_INTEGER := 0;
    v_upd_count     PLS_INTEGER := 0;
    v_del_count     PLS_INTEGER := 0;
    v_batch_ctr     PLS_INTEGER := 0;

    v_dml           VARCHAR2(32767);
    v_log_exists    NUMBER;

    -- =========================================================================
    -- LOCAL SUBPROGRAMS
    -- =========================================================================

    -- -------------------------------------------------------------------------
    -- log_msg
    -- -------------------------------------------------------------------------
    PROCEDURE log_msg (p_msg IN VARCHAR2) IS
    BEGIN
        DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE,'HH24:MI:SS') || ' | ' || p_msg);
    END log_msg;

    -- -------------------------------------------------------------------------
    -- split_row: split a delimited string into an associative array
    -- -------------------------------------------------------------------------
    PROCEDURE split_row (
        p_str   IN  VARCHAR2,
        p_delim IN  VARCHAR2,
        p_out   OUT t_str_tab
    ) IS
        v_str  VARCHAR2(32767) := p_str;
        v_pos  PLS_INTEGER;
        v_idx  PLS_INTEGER := 1;
    BEGIN
        p_out.DELETE;
        LOOP
            v_pos := INSTR(v_str, p_delim);
            IF v_pos = 0 THEN
                p_out(v_idx) := v_str;
                EXIT;
            END IF;
            p_out(v_idx) := SUBSTR(v_str, 1, v_pos - 1);
            v_str        := SUBSTR(v_str, v_pos + LENGTH(p_delim));
            v_idx        := v_idx + 1;
        END LOOP;
    END split_row;

    -- -------------------------------------------------------------------------
    -- quote_val: safely quote a scalar value for dynamic SQL
    -- -------------------------------------------------------------------------
    FUNCTION quote_val (p_val IN VARCHAR2, p_dtype IN VARCHAR2)
    RETURN VARCHAR2 IS
    BEGIN
        IF p_val IS NULL OR p_val = 'NULL' THEN
            RETURN 'NULL';
        ELSIF p_dtype IN ('NUMBER','FLOAT','INTEGER','BINARY_FLOAT','BINARY_DOUBLE') THEN
            RETURN p_val;
        ELSIF p_dtype = 'DATE' THEN
            RETURN 'TO_DATE(''' || p_val || ''',''YYYY-MM-DD HH24:MI:SS'')';
        ELSIF p_dtype LIKE 'TIMESTAMP%' THEN
            RETURN 'TO_TIMESTAMP(''' || p_val || ''',''YYYY-MM-DD HH24:MI:SS.FF'')';
        ELSE
            RETURN '''' || REPLACE(p_val, '''', '''''') || '''';
        END IF;
    END quote_val;

    -- -------------------------------------------------------------------------
    -- row_to_str_remote: fetch one SOURCE row (via DB Link) as CHR(1)-delimited
    -- -------------------------------------------------------------------------
    FUNCTION row_to_str_remote (p_pk_val IN VARCHAR2)
    RETURN VARCHAR2
    IS
        v_sel    VARCHAR2(32767);
        v_wh     VARCHAR2(4000);
        v_result VARCHAR2(32767);
        v_cur    SYS_REFCURSOR;
        v_pks    t_str_tab;
        v_dtype  VARCHAR2(128);
    BEGIN
        split_row(p_pk_val, '|', v_pks);

        -- Rebuild WHERE using remote metadata for PK data types
        FOR i IN 1 .. v_pk_cols.COUNT LOOP
            EXECUTE IMMEDIATE
                'SELECT data_type FROM dba_tab_columns' || v_at_link
                || ' WHERE owner=:1 AND table_name=:2 AND column_name=:3'
            INTO  v_dtype
            USING UPPER(p_src_schema), UPPER(p_src_table), v_pk_cols(i);

            v_wh := v_wh
                 || CASE WHEN i > 1 THEN ' AND ' END
                 || v_pk_cols(i) || ' = '
                 || quote_val(v_pks(i), v_dtype);
        END LOOP;

        -- Concatenate all columns with CHR(1) separator, queried via DB Link
        v_sel := 'SELECT ';
        FOR i IN 1 .. v_cols.COUNT LOOP
            v_sel := v_sel
                  || CASE WHEN i > 1 THEN ' || CHR(1) || ' END
                  || 'NVL(TO_CHAR(' || v_cols(i).column_name || '),''NULL'')';
        END LOOP;
        v_sel := v_sel
              || ' FROM ' || UPPER(p_src_schema) || '.' || UPPER(p_src_table)
              || v_at_link          -- <-- DB Link appended to the remote table
              || ' WHERE ' || v_wh;

        OPEN  v_cur FOR v_sel;
        FETCH v_cur INTO v_result;
        CLOSE v_cur;

        RETURN v_result;
    END row_to_str_remote;

    -- -------------------------------------------------------------------------
    -- row_to_str_local: fetch one TARGET row (local) as CHR(1)-delimited
    -- -------------------------------------------------------------------------
    FUNCTION row_to_str_local (p_pk_val IN VARCHAR2)
    RETURN VARCHAR2
    IS
        v_sel    VARCHAR2(32767);
        v_wh     VARCHAR2(4000);
        v_result VARCHAR2(32767);
        v_cur    SYS_REFCURSOR;
        v_pks    t_str_tab;
        v_dtype  VARCHAR2(128);
    BEGIN
        split_row(p_pk_val, '|', v_pks);

        FOR i IN 1 .. v_pk_cols.COUNT LOOP
            SELECT data_type INTO v_dtype
            FROM   dba_tab_columns
            WHERE  owner       = UPPER(p_tgt_schema)
              AND  table_name  = UPPER(p_tgt_table)
              AND  column_name = v_pk_cols(i);

            v_wh := v_wh
                 || CASE WHEN i > 1 THEN ' AND ' END
                 || v_pk_cols(i) || ' = '
                 || quote_val(v_pks(i), v_dtype);
        END LOOP;

        v_sel := 'SELECT ';
        FOR i IN 1 .. v_cols.COUNT LOOP
            v_sel := v_sel
                  || CASE WHEN i > 1 THEN ' || CHR(1) || ' END
                  || 'NVL(TO_CHAR(' || v_cols(i).column_name || '),''NULL'')';
        END LOOP;
        v_sel := v_sel
              || ' FROM ' || v_tgt_full   -- local, no DB Link
              || ' WHERE ' || v_wh;

        OPEN  v_cur FOR v_sel;
        FETCH v_cur INTO v_result;
        CLOSE v_cur;

        RETURN v_result;
    END row_to_str_local;

    -- -------------------------------------------------------------------------
    -- ensure_log_table
    -- -------------------------------------------------------------------------
    PROCEDURE ensure_log_table IS
    BEGIN
        SELECT COUNT(*) INTO v_log_exists
        FROM   user_tables
        WHERE  table_name = 'SYNC_LOG';

        IF v_log_exists = 0 THEN
            EXECUTE IMMEDIATE '
                CREATE TABLE sync_log (
                    log_id      NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    run_time    DATE          DEFAULT SYSDATE,
                    db_link     VARCHAR2(128),
                    src_table   VARCHAR2(400),
                    tgt_table   VARCHAR2(261),
                    operation   VARCHAR2(10),
                    pk_values   VARCHAR2(4000),
                    status      VARCHAR2(10),
                    error_msg   VARCHAR2(4000)
                )';
            log_msg('Created SYNC_LOG table.');
        END IF;
    END ensure_log_table;

    -- -------------------------------------------------------------------------
    -- apply_or_print
    -- -------------------------------------------------------------------------
    PROCEDURE apply_or_print (
        p_op     IN VARCHAR2,
        p_pk_val IN VARCHAR2,
        p_sql    IN VARCHAR2
    ) IS
    BEGIN
        IF p_mode = 'PREVIEW' THEN
            DBMS_OUTPUT.PUT_LINE('-- [' || p_op || '] pk=' || p_pk_val);
            DBMS_OUTPUT.PUT_LINE(p_sql || ';');
            DBMS_OUTPUT.PUT_LINE('');
        ELSE
            BEGIN
                EXECUTE IMMEDIATE p_sql;

                EXECUTE IMMEDIATE
                    'INSERT INTO sync_log
                         (db_link, src_table, tgt_table, operation, pk_values, status)
                     VALUES (:1,:2,:3,:4,:5,''OK'')'
                USING v_db_link, v_src_full, v_tgt_full, p_op, p_pk_val;

                v_batch_ctr := v_batch_ctr + 1;
                IF v_batch_ctr >= p_batch_size THEN
                    COMMIT;
                    v_batch_ctr := 0;
                    log_msg('  Batch commit at ' || p_batch_size || ' rows.');
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    EXECUTE IMMEDIATE
                        'INSERT INTO sync_log
                             (db_link, src_table, tgt_table, operation,
                              pk_values, status, error_msg)
                         VALUES (:1,:2,:3,:4,:5,''ERROR'',:6)'
                    USING v_db_link, v_src_full, v_tgt_full,
                          p_op, p_pk_val, SUBSTR(SQLERRM,1,4000);
                    log_msg('  ERROR ' || p_op
                          || ' pk=' || p_pk_val || ' -> ' || SQLERRM);
            END;
        END IF;
    END apply_or_print;

-- =============================================================================
-- MAIN BODY
-- =============================================================================
BEGIN
    DBMS_OUTPUT.ENABLE(1000000);

    -- Strip any leading '@' the caller may have included, then rebuild
    v_db_link := UPPER(LTRIM(TRIM(p_db_link), '@'));
    v_at_link := '@' || v_db_link;

    -- Full qualified names
    v_src_full := UPPER(p_src_schema) || '.' || UPPER(p_src_table) || v_at_link;
    v_tgt_full := UPPER(p_tgt_schema) || '.' || UPPER(p_tgt_table);

    log_msg('=== SYNC_TABLES START ===');
    log_msg('Source  : ' || v_src_full || '  (remote via DB Link)');
    log_msg('Target  : ' || v_tgt_full || '  (local)');
    log_msg('Mode    : ' || p_mode);

    IF p_mode NOT IN ('EXECUTE','PREVIEW') THEN
        RAISE_APPLICATION_ERROR(-20001, 'p_mode must be EXECUTE or PREVIEW');
    END IF;

    -- -------------------------------------------------------------------------
    -- Validate DB Link exists locally before doing anything else
    -- -------------------------------------------------------------------------
    DECLARE
        v_link_count NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_link_count
        FROM   all_db_links
        WHERE  db_link = v_db_link
           OR  db_link = v_db_link || '.' ||
               (SELECT value FROM v$parameter WHERE name='db_domain');

        IF v_link_count = 0 THEN
            RAISE_APPLICATION_ERROR(-20010,
                'DB Link "' || v_db_link || '" not found in ALL_DB_LINKS. '
                || 'Create it first: CREATE DATABASE LINK ' || v_db_link
                || ' CONNECT TO <user> IDENTIFIED BY <pwd> USING ''<tns_alias'';');
        END IF;
        log_msg('DB Link "' || v_db_link || '" validated OK.');
    END;

    IF p_mode = 'EXECUTE' THEN
        ensure_log_table;
    END IF;

    -- =========================================================================
    -- Step 1: Load column metadata from the REMOTE DB via DB Link
    -- =========================================================================
    log_msg('Step 1: Reading remote column metadata via ' || v_at_link || ' ...');

    DECLARE
        v_meta_sql  VARCHAR2(4000);
        v_meta_cur  SYS_REFCURSOR;
        v_col_name  VARCHAR2(128);
        v_dtype     VARCHAR2(128);
        v_is_pk     VARCHAR2(1);
        v_idx       PLS_INTEGER := 1;
    BEGIN
        -- Query DBA_TAB_COLUMNS and DBA_CONSTRAINTS on the REMOTE side
        v_meta_sql :=
            'SELECT tc.column_name, tc.data_type,'
            || ' CASE WHEN pk.column_name IS NOT NULL THEN ''Y'' ELSE ''N'' END'
            || ' FROM dba_tab_columns' || v_at_link || ' tc'
            || ' LEFT JOIN ('
            ||     ' SELECT cc.column_name'
            ||     ' FROM dba_cons_columns'  || v_at_link || ' cc'
            ||     ' JOIN  dba_constraints'  || v_at_link || ' c'
            ||         ' ON c.constraint_name = cc.constraint_name'
            ||         ' AND c.owner = cc.owner'
            ||     ' WHERE c.constraint_type = ''P'''
            ||     '   AND c.owner      = ''' || UPPER(p_src_schema) || ''''
            ||     '   AND cc.table_name= ''' || UPPER(p_src_table)  || ''''
            || ' ) pk ON pk.column_name = tc.column_name'
            || ' WHERE tc.owner      = ''' || UPPER(p_src_schema) || ''''
            ||   ' AND tc.table_name = ''' || UPPER(p_src_table)  || ''''
            || ' ORDER BY tc.column_id';

        OPEN v_meta_cur FOR v_meta_sql;
        LOOP
            FETCH v_meta_cur INTO v_col_name, v_dtype, v_is_pk;
            EXIT WHEN v_meta_cur%NOTFOUND;

            v_cols(v_idx).column_name := v_col_name;
            v_cols(v_idx).data_type   := v_dtype;
            v_cols(v_idx).is_pk       := v_is_pk;

            IF v_is_pk = 'Y' THEN
                v_pk_cols.EXTEND;
                v_pk_cols(v_pk_cols.LAST) := v_col_name;
            ELSE
                v_data_cols.EXTEND;
                v_data_cols(v_data_cols.LAST) := v_col_name;
            END IF;
            v_idx := v_idx + 1;
        END LOOP;
        CLOSE v_meta_cur;
    END;

    IF v_cols.COUNT = 0 THEN
        RAISE_APPLICATION_ERROR(-20002,
            'No columns found for ' || v_src_full
            || '. Check schema/table name and SELECT on DBA_TAB_COLUMNS'
            || v_at_link);
    END IF;
    IF v_pk_cols.COUNT = 0 THEN
        RAISE_APPLICATION_ERROR(-20003,
            'No primary key found on ' || v_src_full
            || '. A PK is required for row-level comparison.');
    END IF;

    log_msg('  Total columns : ' || v_cols.COUNT);
    log_msg('  PK columns    : ' || v_pk_cols.COUNT);
    log_msg('  Data columns  : ' || v_data_cols.COUNT);

    -- =========================================================================
    -- Step 2: Build reusable SQL fragments
    -- =========================================================================
    log_msg('Step 2: Building SQL fragments ...');

    FOR i IN 1 .. v_cols.COUNT LOOP
        v_col_list := v_col_list
                   || CASE WHEN i > 1 THEN ', ' END
                   || v_cols(i).column_name;
    END LOOP;

    -- PK join: s.PK1 = t.PK1 AND ...
    -- Note: s comes from the remote table, t from local – Oracle resolves
    -- DB Link context from the FROM clause, not the JOIN predicate.
    FOR i IN 1 .. v_pk_cols.COUNT LOOP
        v_pk_join := v_pk_join
                  || CASE WHEN i > 1 THEN ' AND ' END
                  || 's.' || v_pk_cols(i) || ' = t.' || v_pk_cols(i);
    END LOOP;

    IF v_data_cols.COUNT > 0 THEN
        FOR i IN 1 .. v_data_cols.COUNT LOOP
            -- Use DECODE for NULL-safe comparison – valid across DB Links.
            -- DECODE(s.C, t.C, 0, 1) treats NULL=NULL as equal (returns 0),
            -- and NULL!=value or value!=NULL as different (returns 1).
            v_data_compare := v_data_compare
                           || CASE WHEN i > 1 THEN ' + ' END
                           || 'DECODE(s.' || v_data_cols(i)
                           || ', t.'      || v_data_cols(i)
                           || ', 0, 1)';
        END LOOP;
    END IF;

    -- =========================================================================
    -- Step 3: Identify and process differences
    --   Source table always referenced as SCHEMA.TABLE@DBLINK (v_src_full)
    --   Target table always local (v_tgt_full)
    -- =========================================================================
    log_msg('Step 3: Comparing remote source to local target ...');

    DECLARE
        v_diff_cur  SYS_REFCURSOR;
        v_diff_sql  VARCHAR2(32767);
        v_op        VARCHAR2(10);
        v_pk_val    VARCHAR2(4000);
        v_pk_sel_s  VARCHAR2(4000);   -- serialised PK from source alias s
        v_pk_sel_t  VARCHAR2(4000);   -- serialised PK from target alias t
    BEGIN
        FOR i IN 1 .. v_pk_cols.COUNT LOOP
            v_pk_sel_s := v_pk_sel_s
                       || CASE WHEN i > 1 THEN ' || ''|'' || ' END
                       || 'TO_CHAR(s.' || v_pk_cols(i) || ')';
            v_pk_sel_t := v_pk_sel_t
                       || CASE WHEN i > 1 THEN ' || ''|'' || ' END
                       || 'TO_CHAR(t.' || v_pk_cols(i) || ')';
        END LOOP;

        -- -----------------------------------------------------------------------
        -- Build a NULL-check on the first PK column of each side.
        -- Used in LEFT JOIN ... WHERE pk IS NULL to detect missing rows.
        -- Oracle does NOT support NOT EXISTS across a DB Link reliably,
        -- so we use LEFT JOIN with IS NULL instead.
        -- -----------------------------------------------------------------------
        DECLARE
            v_pk1_s VARCHAR2(128) := 's.' || v_pk_cols(1);
            v_pk1_t VARCHAR2(128) := 't.' || v_pk_cols(1);
        BEGIN
            -- -----------------------------------------------------------------------
            -- INSERTs: rows in remote source with no matching row in local target
            -- LEFT JOIN local target onto remote source; NULLs in t = missing locally
            -- -----------------------------------------------------------------------
            v_diff_sql :=
                'SELECT ''INSERT'', ' || v_pk_sel_s
                || ' FROM '      || v_src_full || ' s'       -- remote
                || ' LEFT JOIN ' || v_tgt_full || ' t'       -- local
                ||     ' ON ('   || v_pk_join  || ')'
                || ' WHERE '     || v_pk1_t    || ' IS NULL'

            -- -----------------------------------------------------------------------
            -- DELETEs: rows in local target with no matching row in remote source
            -- LEFT JOIN remote source onto local target; NULLs in s = deleted remotely
            -- -----------------------------------------------------------------------
                || ' UNION ALL '
                || 'SELECT ''DELETE'', ' || v_pk_sel_t
                || ' FROM '      || v_tgt_full || ' t'       -- local
                || ' LEFT JOIN ' || v_src_full || ' s'       -- remote
                ||     ' ON ('   || v_pk_join  || ')'
                || ' WHERE '     || v_pk1_s    || ' IS NULL';

            -- -----------------------------------------------------------------------
            -- UPDATEs: rows present in both but with at least one changed data column
            -- INNER JOIN is fine here since both rows must exist
            -- -----------------------------------------------------------------------
            IF v_data_cols.COUNT > 0 THEN
                v_diff_sql := v_diff_sql
                    || ' UNION ALL '
                    || 'SELECT ''UPDATE'', ' || v_pk_sel_s
                    || ' FROM '     || v_src_full || ' s'    -- remote
                    || ' JOIN '     || v_tgt_full || ' t'    -- local
                    ||     ' ON ('  || v_pk_join  || ')'
                    || ' WHERE ('   || v_data_compare || ') > 0';
            END IF;
        END;

        OPEN v_diff_cur FOR v_diff_sql;
        LOOP
            FETCH v_diff_cur INTO v_op, v_pk_val;
            EXIT WHEN v_diff_cur%NOTFOUND;

            DECLARE
                v_src_str   VARCHAR2(32767);
                v_tgt_str   VARCHAR2(32767);
                v_src_vals  t_str_tab;
                v_tgt_vals  t_str_tab;
                v_set       VARCHAR2(32767);
                v_where     VARCHAR2(32767);
                v_ins_vals  VARCHAR2(32767);
            BEGIN
                CASE v_op

                -- ---------------------------------------------------------------
                WHEN 'INSERT' THEN   -- data comes from remote source
                -- ---------------------------------------------------------------
                    v_src_str := row_to_str_remote(v_pk_val);
                    split_row(v_src_str, CHR(1), v_src_vals);

                    v_ins_vals := '';
                    FOR i IN 1 .. v_cols.COUNT LOOP
                        v_ins_vals := v_ins_vals
                                   || CASE WHEN i > 1 THEN ', ' END
                                   || quote_val(v_src_vals(i), v_cols(i).data_type);
                    END LOOP;

                    v_dml := 'INSERT INTO ' || v_tgt_full    -- local target
                          || ' (' || v_col_list || ')'
                          || ' VALUES (' || v_ins_vals || ')';

                    apply_or_print('INSERT', v_pk_val, v_dml);
                    v_ins_count := v_ins_count + 1;

                -- ---------------------------------------------------------------
                WHEN 'UPDATE' THEN   -- compare remote src vs local tgt
                -- ---------------------------------------------------------------
                    v_src_str := row_to_str_remote(v_pk_val);
                    v_tgt_str := row_to_str_local(v_pk_val);
                    split_row(v_src_str, CHR(1), v_src_vals);
                    split_row(v_tgt_str, CHR(1), v_tgt_vals);

                    v_set   := '';
                    v_where := '';

                    FOR i IN 1 .. v_cols.COUNT LOOP
                        IF v_cols(i).is_pk = 'N' THEN
                            IF NVL(v_src_vals(i),'NULL') != NVL(v_tgt_vals(i),'NULL') THEN
                                v_set := v_set
                                      || CASE WHEN LENGTH(v_set) > 0 THEN ', ' END
                                      || v_cols(i).column_name || ' = '
                                      || quote_val(v_src_vals(i), v_cols(i).data_type);
                            END IF;
                        END IF;
                    END LOOP;

                    FOR i IN 1 .. v_cols.COUNT LOOP
                        IF v_cols(i).is_pk = 'Y' THEN
                            v_where := v_where
                                    || CASE WHEN LENGTH(v_where) > 0 THEN ' AND ' END
                                    || v_cols(i).column_name || ' = '
                                    || quote_val(v_src_vals(i), v_cols(i).data_type);
                        END IF;
                    END LOOP;

                    v_dml := 'UPDATE ' || v_tgt_full         -- local target
                          || ' SET '   || v_set
                          || ' WHERE ' || v_where;

                    apply_or_print('UPDATE', v_pk_val, v_dml);
                    v_upd_count := v_upd_count + 1;

                -- ---------------------------------------------------------------
                WHEN 'DELETE' THEN   -- data comes from local target
                -- ---------------------------------------------------------------
                    v_tgt_str := row_to_str_local(v_pk_val);
                    split_row(v_tgt_str, CHR(1), v_tgt_vals);

                    v_where := '';
                    FOR i IN 1 .. v_cols.COUNT LOOP
                        IF v_cols(i).is_pk = 'Y' THEN
                            v_where := v_where
                                    || CASE WHEN LENGTH(v_where) > 0 THEN ' AND ' END
                                    || v_cols(i).column_name || ' = '
                                    || quote_val(v_tgt_vals(i), v_cols(i).data_type);
                        END IF;
                    END LOOP;

                    v_dml := 'DELETE FROM ' || v_tgt_full    -- local target
                          || ' WHERE '      || v_where;

                    apply_or_print('DELETE', v_pk_val, v_dml);
                    v_del_count := v_del_count + 1;

                END CASE;
            END;
        END LOOP;
        CLOSE v_diff_cur;
    END;

    -- =========================================================================
    -- Step 4: Final commit and summary
    -- =========================================================================
    IF p_mode = 'EXECUTE' AND v_batch_ctr > 0 THEN
        COMMIT;
    END IF;

    log_msg('');
    log_msg('=== SYNC SUMMARY ===');
    log_msg('  DB Link : ' || v_db_link);
    log_msg('  Inserts : ' || v_ins_count);
    log_msg('  Updates : ' || v_upd_count);
    log_msg('  Deletes : ' || v_del_count);
    log_msg('  Total   : ' || (v_ins_count + v_upd_count + v_del_count));
    IF p_mode = 'EXECUTE' THEN
        log_msg('  Status  : Changes applied and committed.');
        log_msg('  Log     : SELECT * FROM sync_log ORDER BY log_id DESC;');
    ELSE
        log_msg('  Status  : PREVIEW only – no changes were applied.');
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
-- SETUP: Create the DB Link (run once as DBA or the owning schema)
-- =============================================================================
-- CREATE DATABASE LINK prod_link
--   CONNECT TO remote_user IDENTIFIED BY remote_pwd
--   USING 'PROD_TNS_ALIAS';       -- must exist in tnsnames.ora or LDAP


-- =============================================================================
-- USAGE EXAMPLES
-- =============================================================================

-- 1. Preview: prints all SQL – nothing is applied
SET SERVEROUTPUT ON SIZE UNLIMITED;
BEGIN
    sync_tables(
        p_src_schema => 'HR',
        p_src_table  => 'EMPLOYEES',
        p_db_link    => 'PROD_LINK',      -- <<< DB Link name
        p_tgt_schema => 'HR_BACKUP',
        p_tgt_table  => 'EMPLOYEES',
        p_mode       => 'PREVIEW'
    );
END;
/

-- 2. Execute: apply changes with batch commits of 500 rows
BEGIN
    sync_tables(
        p_src_schema => 'HR',
        p_src_table  => 'EMPLOYEES',
        p_db_link    => 'PROD_LINK',
        p_tgt_schema => 'HR_BACKUP',
        p_tgt_table  => 'EMPLOYEES',
        p_mode       => 'EXECUTE',
        p_batch_size => 500
    );
END;
/

-- 3. Review audit log after execution
SELECT db_link, operation, pk_values, status, error_msg, run_time
FROM   sync_log
ORDER  BY log_id DESC;
