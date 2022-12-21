#!/bin/bash

#--------------------------------------------------------------------------
# qprof version 0.5a - Dec 2022 - maurizio.felici@vertica.com
# vim: et:ts=4:sw=4:sm:ai
#--------------------------------------------------------------------------

#---------------------------------------------------------------------------
# Setting Default Values
#---------------------------------------------------------------------------
QPV="qprof version 0.5a - Dec 2022"
OUT=qprof.out
CLC=0
GZP=0
RPOOL=""
usage="Usage: qprof {-f query_file | -s statement_id -t transaction_id} [-o output_file] [-rp resource_pool] [-cc] [-gz]\n"
usage+=" -f query_file (it should not contain the 'profile' keyword\n"
usage+=" -s statement_id -t transaction_id is alternative to '-f file'\n"
usage+=" -o  output_file to set the output file (default sprof.out)\n"
usage+=" -rp resource_pool to set the resource pool where the query in '-f file' is executed\n"
usage+=" -gz to gzip the output file\n"
usage+=" -cc to clear the cache"

#---------------------------------------------------------------------------
# Check user has dbadmin role
#---------------------------------------------------------------------------
if [ $(vsql -X -A -t -q -c "SELECT has_role('dbadmin')") == 'f' ] ; then
    echo "User has no dbadmin role"
    exit 1
fi

#---------------------------------------------------------------------------
# Command line options handling
#---------------------------------------------------------------------------
test $# -eq 0 && { echo -e $usage ; exit ; }
while [ $# -gt 0 ]; do
    case "$1" in
        "-rp")
            RPOOL="SET SESSION RESOURCE POOL $2 ;"
            shift 2
            ;;
        "-o")
            OUT=$2
            shift 2
            ;;
        "-s")
            SID=$2
            shift 2
            ;;
        "-t")
            TID=$2
            shift 2
            ;;
        "-f")
            SQL=$2
            test ! -f ${SQL} && { echo Cannot read ${SQL} ; exit 1 ; }
            shift 2
            ;;
        "-cc")
            CLC=1
            shift
            ;;
        "-gz")
            GZP=1
            shift
            ;;
        "--help" | "-h")
            echo -e $usage
            exit 0
            ;;
        *)
            echo "[qprof] invalid option '$1'"
            echo -e $usage
            exit 1
            ;;
    esac
done

#---------------------------------------------------------------------------
# Running Query (output to /dev/null) if any...
#---------------------------------------------------------------------------
if [ ${CLC} -eq 1 ] ; then
    echo "[qprof] Clear Linux cache..."
    sudo sh -c "sync && echo 3 > /proc/sys/vm/drop_caches"
    echo "[qprof] Clear Vertica Internal cache..."
    vsql -X -A -q -f - -o /dev/null -c "SELECT CLEAR_CACHES()"
fi
if [ ! -z ${SQL+x} ] ; then
    echo "[qprof] Running query $SQL (result set redirected to /dev/null)"
    trxst=$(sed "1s/^/${RPOOL} PROFILE /" ${SQL} | 
            vsql -X -A -q -f - -o /dev/null 2>&1 | 
            sed -n 's/^HINT:.*=\([0-9]*\).*=\([0-9]*\);$/\1,\2/p' |
            tail -n1 )  # to get the last line (second execution if replanned)
    if [ -z "${trxst}" ] ; then
        echo "[qprof] No Transaction/Statement ID retrieved. Check your SQL"
        exit 2
    fi
    TID=${trxst%,*}
    SID=${trxst#*,}
elif [[ -z ${SID+x} || -z ${TID+x} ]] ; then
    echo "[qprof] You should provide either a SQL file (-f) or Statement/Transaction ID (-s and -t)"
    exit 1
else
    :
fi

#---------------------------------------------------------------------------
# Running profile analysis
#---------------------------------------------------------------------------
echo "[qprof] Analyzing query profile. Output to ${OUT}"
cat <<-EOF | vsql -X -q -P null='(null)' -o ${OUT} -f -
    \set trxid $TID
    \set stmtid $SID
    \set qpv "${QPV}"
    \qecho +-------------------------------------------------------------------
    \qecho | Date: `date` :qpv
    \qecho | Transaction ID: :trxid
    \qecho | Statement ID: :stmtid
    \qecho | Clear Cache: ${CLC}
    \qecho | Query File: ${SQL}
    \qecho +-------------------------------------------------------------------

    -- ------------------------------------------------------------------------
    -- Vertica Version
    -- ------------------------------------------------------------------------
    \echo '    Step 00: Vertica version'
    \qecho >>> Step 00: Vertica version
    SELECT
        VERSION() 
    ;

    -- ------------------------------------------------------------------------
    -- Query text
    -- ------------------------------------------------------------------------
    \echo '    Step 01: Query text'
    \qecho >>> Step 01: Query text
    \pset tuples_only
    SELECT
        request 
    FROM 
        v_internal.dc_requests_issued 
    WHERE  
        transaction_id=:trxid AND 
        statement_id=:stmtid 
    ;
    \pset tuples_only

    -- ------------------------------------------------------------------------
    -- Query duration
    -- ------------------------------------------------------------------------
    \echo '    Step 02: Query duration'
    \qecho >>> Step 02: Query duration
    SELECT
        query_duration_us 
    FROM 
        v_monitor.query_profiles 
    WHERE 
        transaction_id=:trxid AND 
        statement_id=:stmtid
    ;

    -- ------------------------------------------------------------------------
    -- Query execution steps
    -- ------------------------------------------------------------------------
    \echo '    Step 03: Query execution steps'
    \qecho >>> Step 03: Query execution steps
    SELECT
        execution_step, 
        MAX(completion_time - time) AS elapsed
    FROM 
        v_internal.dc_query_executions 
    WHERE 
        transaction_id=:trxid AND 
        statement_id=:stmtid 
    GROUP BY 
        1
    ORDER BY
        2 desc
    ;

    -- ------------------------------------------------------------------------
    -- Resource Acquisition
    -- ------------------------------------------------------------------------
    \echo '    Step 04: Resource Acquisition'
    \qecho >>> Step 04: Resource Acquisition
    SELECT
        a.node_name,
        a.queue_entry_timestamp,
        a.acquisition_timestamp,
        ( a.acquisition_timestamp - a.queue_entry_timestamp ) AS queue_wait_time,
        a.pool_name, 
        a.memory_inuse_kb AS mem_kb,
        (b.reserved_extra_memory_b/1000)::INTEGER AS emem_kb,
        (a.memory_inuse_kb-b.reserved_extra_memory_b/1000)::INTEGER AS rmem_kb,
        a.open_file_handle_count AS fhc,
        a.thread_count AS threads
    FROM 
        v_monitor.resource_acquisitions a
        INNER JOIN query_profiles b
            ON a.transaction_id = b.transaction_id
    WHERE 
        a.transaction_id=:trxid AND 
        a.statement_id=:stmtid 
    ORDER BY
        1, 2
    ;
    \qecho Please note:
    \qecho mem_kb = memory acquired by this Q
    \qecho emem_kb = unused memory acquired by this Q
    \qecho rmem_kb = memory used by this Q
    \qecho fhc = file handles count
    \qecho

    -- ------------------------------------------------------------------------
    -- Query plan
    -- ------------------------------------------------------------------------
    \echo '    Step 05: Query execution plan'
    \qecho >>> Step 05: Query execution plan
    SELECT
        statement_id AS stmtid,
        path_line 
    FROM 
        v_internal.dc_explain_plans
    WHERE 
        transaction_id=:trxid 
    ORDER BY 
        statement_id,
        path_id,
        path_line_index
    ;

    -- ------------------------------------------------------------------------
    -- Query plan
    -- ------------------------------------------------------------------------
    \echo '    Step 06: Query plan profile'
    \qecho >>> Step 06: Query plan profile
    SELECT
        statement_id AS stmtid,
        path_id,
        running_time,
        (memory_allocated_bytes//(1024*1024))::numeric(18,2) AS mem_mb,
        (read_from_disk_bytes//(1024*1024))::numeric(18,2) AS read_mb,
        (received_bytes//(1024*1024))::numeric(18,2) AS in_mb,
        (sent_bytes//(1024*1024))::numeric(18,2) AS out_mb,
        left(path_line, 80) as path_line
    FROM v_monitor.query_plan_profiles
    WHERE transaction_id = :trxid
    ORDER BY
        statement_id,
        path_id,
        path_line_index ;

    -- ------------------------------------------------------------------------
    -- Query consumption
    -- ------------------------------------------------------------------------
    \pset expanded
    \echo '    Step 07: Query consumption'
    \qecho >>> Step 07: Query consumption
    SELECT
        *
    FROM v_monitor.query_consumption
    WHERE transaction_id = :trxid
    ORDER BY
        statement_id;
    \pset expanded

    -- ------------------------------------------------------------------------
    -- Elapsed & memory allocated by node and path_id
    -- ------------------------------------------------------------------------
    \echo '    Step 08: Elapsed & memory allocated by node, path_id and activity'
    \qecho >>> Step 08: Elapsed & memory allocated by node, path_id and activity
    SELECT
        a.node_name, 
        a.path_id, 
        a.baseplan_id::VARCHAR || ',' || a.localplan_id::VARCHAR AS bl_id,
        b.path_line,
        a.description, 
        (a.assigned_memory_bytes/(1024*1024))::numeric(18,2) AS mem_mb 
    FROM 
        v_internal.dc_plan_resources a 
        INNER JOIN
        (SELECT
            path_id,
            regexp_replace(path_line,'^[^A-Z]*(.*)\[.*$','\1') AS path_line 
         FROM 
            v_internal.dc_explain_plans
         WHERE 
            path_line_index = 1 AND
            transaction_id=:trxid AND 
            statement_id=:stmtid 
        ) b
        ON a.path_id = b.path_id
    WHERE 
        a.transaction_id=:trxid AND 
        a.statement_id=:stmtid 
    ORDER BY
        1 asc, 2 asc, 5 desc
    ;
    \qecho Please note:
    \qecho bl_id = baseplan_id,localplan_id

    -- ------------------------------------------------------------------------
    -- Elapsed, exec_time and I/O by node, activity and path_id
    -- ------------------------------------------------------------------------
    \echo '    Step 09: Elapsed, exec_time and I/O by node, activity & path_id'
    \qecho >>> Step 09: Elapsed, exec_time and I/O by node, activity & path_id
    SELECT
        node_name,
        path_id,
        activity,
        activity_id::VARCHAR || ',' || baseplan_id::VARCHAR || ',' || localplan_id::VARCHAR AS abl_id,
        TIMESTAMPDIFF( us , start_time, end_time) AS elaps_us, 
        execution_time_us AS exec_us,
        CASE WHEN associated_oid IS NOT NULL THEN description ELSE NULL END AS input,
        input_size_rows AS input_rows,
        ROUND(input_size_bytes/1000000, 2.0) AS input_mb,
        processed_rows AS proc_rows,
        ROUND(processed_bytes/1000000, 2.0) AS proc_mb
    FROM
        v_internal.dc_plan_activities
    WHERE
        transaction_id=:trxid AND
        statement_id=:stmtid
    ORDER BY
        1, 2, 4
        ;
    \qecho Please note:
    \qecho abl_id = activity_id,baseplan_id,localplan_id

    -- ------------------------------------------------------------------------
    -- Query events
    -- ------------------------------------------------------------------------
    \pset expanded
    \echo '    Step 10: Query events'
    \qecho >>> Step 10: Query events
    SELECT 
        event_timestamp, 
        node_name, 
        event_category, 
        event_type, 
        event_description, 
        operator_name, 
        path_id,
        event_details, 
        suggested_action 
    FROM 
        v_monitor.query_events 
    WHERE 
        transaction_id=:trxid AND 
        statement_id=:stmtid 
    ORDER BY 
        1
    ;
    \pset expanded

    -- ------------------------------------------------------------------------
    -- Suggested Action Summary
    -- ------------------------------------------------------------------------
    \echo '    Step 11: Suggested Action Summary'
    \qecho >>> Step 11: Suggested Action Summary
    SELECT
        path_id, 
        MAX(suggested_action) AS 'Suggested Action' 
    FROM 
        v_monitor.query_events 
    WHERE 
        suggested_action <> '' AND 
        transaction_id = :trxid AND 
        statement_id = :stmtid 
    GROUP BY 1 
    ORDER BY 1  
    ;

    -- ------------------------------------------------------------------------
    -- CPU Time by node and path_id
    -- ------------------------------------------------------------------------
    \echo '    Step 12: CPU Time by node and path_id'
    \qecho >>> Step 12: CPU Time by node and path_id
    SELECT
        node_name, 
        path_id, 
        cet, 
        (100*cet/(SUM(cet) over(partition by node_name)))::numeric(6,3) AS tot_node_cpu_perc
    FROM ( 
        SELECT 
            node_name, 
            path_id, 
            SUM(counter_value) AS cet 
        FROM 
            v_monitor.execution_engine_profiles 
        WHERE 
            counter_name = 'execution time (us)' AND 
            transaction_id=:trxid AND 
            statement_id=:stmtid 
        GROUP BY 1, 2 ) x 
    ORDER BY 1, 2 ; 
    \qecho Please Note:
    \qecho tot_node_cpu_perc is the % of cpu cycles SPENT on this node
    \qecho and NOT the % of cpu cycles AVAILABLE on this node.

    -- ------------------------------------------------------------------------
    -- Threads per operator by node & path_id
    -- ------------------------------------------------------------------------
    \echo '    Step 13: Threads by node and path_id'
    \qecho >>> Step 13: Threads by node and path_id
    SELECT
        node_name, 
        path_id,
        operator_name, 
        COUNT(DISTINCT operator_id) AS '#Threads'
    FROM 
        v_monitor.execution_engine_profiles 
    WHERE 
        transaction_id=:trxid AND 
        statement_id=:stmtid
    GROUP BY 
        1, 2, 3
    ORDER BY
        1, 2, 3;
    \qecho Please Note:
    \qecho abl_id = activity_id,baseplan_id,localplan_id

    -- ------------------------------------------------------------------------
    -- Query execution report
    -- ------------------------------------------------------------------------
    \echo '    Step 14: Query execution report'
    \qecho >>> Step 14: Query execution report
    SELECT
            node_name ,
            operator_name,
            path_id,
            ROUND(SUM(CASE counter_name WHEN 'execution time (us)' THEN
                counter_value ELSE NULL END)/1000,3.0) AS exec_time_ms,
            SUM(CASE counter_name WHEN 'estimated rows produced' THEN
                counter_value ELSE NULL END ) AS est_rows,
            SUM ( CASE counter_name WHEN 'rows processed' THEN
                counter_value ELSE NULL END ) AS proc_rows,
            SUM ( CASE counter_name WHEN 'rows produced' THEN
                counter_value ELSE NULL END ) AS prod_rows,
            SUM ( CASE counter_name WHEN 'rle rows produced' THEN
                counter_value ELSE NULL END ) AS rle_prod_rows,
            SUM ( CASE counter_name WHEN 'consumer stall (us)' THEN
                counter_value ELSE NULL END ) AS cstall_us,
            SUM ( CASE counter_name WHEN 'producer stall (us)' THEN
                counter_value ELSE NULL END ) AS pstall_us,
            ROUND(SUM(CASE counter_name WHEN 'memory reserved (bytes)' THEN
                counter_value ELSE NULL END)/1000000,1.0) AS mem_res_mb,
            ROUND(SUM(CASE counter_name WHEN 'memory allocated (bytes)' THEN 
                counter_value ELSE NULL END )/1000000,1.0) AS mem_all_mb
    FROM
            v_monitor.execution_engine_profiles
    WHERE
            transaction_id = :trxid AND
            statement_id = :stmtid AND
            counter_value/1000000 > 0
    GROUP BY
            1, 2, 3
    ORDER BY
            -- NULL values at the end...
            CASE WHEN SUM(CASE counter_name WHEN 'execution time (us)' THEN
                counter_value ELSE NULL END) IS NULL THEN 1 ELSE 0 END asc ,
            5 desc
    ;

    -- ------------------------------------------------------------------------
    -- Transaction locks
    -- ------------------------------------------------------------------------
    \echo '    Step 15: Transaction locks'
    \qecho >>> Step 15: Transaction locks
    SELECT
        node_name,
        (time - start_time) AS lock_wait,
        object_name,
        mode,
        promoted_mode,
        scope,
        result,
        description
    FROM
        v_internal.dc_lock_attempts
    WHERE
        transaction_id = :trxid
    ;

    -- ------------------------------------------------------------------------
    -- Projection Data Distribution
    -- ------------------------------------------------------------------------
    \echo '    Step 16: Projection Data Distribution'
    \qecho >>> Step 16: Projection Data Distribution
    SELECT
        p.anchor_table_schema || '.' || p.anchor_table_name AS table_name,
        s.projection_name,
        s.node_name,
        SUM(s.row_count) AS row_count, 
        SUM(s.used_bytes) AS used_bytes,
        SUM(s.ros_count) AS ROS_count,
        SUM(sc.deleted_row_count) AS del_rows,
        SUM(sc.delete_vector_count) AS DV_count
    FROM
        v_monitor.projection_storage s
        INNER JOIN v_monitor.projection_usage p
          ON s.projection_id = p.projection_id
        INNER JOIN v_monitor.storage_containers sc
          ON s.projection_id = sc.projection_id
        INNER JOIN v_catalog.projections pj
          ON s.projection_id = pj.projection_id
    WHERE
        p.statement_id=:stmtid AND
        p.transaction_id=:trxid AND
        pj.is_segmented IS true
    GROUP BY 1, 2, 3
    UNION ALL
    SELECT
        DISTINCT 
            p.anchor_table_schema || '.' || p.anchor_table_name AS table_name,
            s.projection_name,
            'all (unsegmented)' AS node_name,
            s.row_count AS row_count, 
            s.used_bytes AS used_bytes,
            s.ros_count AS ROS_count,
            sc.deleted_row_count AS del_rows,
            sc.delete_vector_count AS DV_count
    FROM
        v_monitor.projection_usage p
        INNER JOIN v_monitor.projection_storage s
          ON s.projection_id = p.projection_id 
        INNER JOIN v_monitor.storage_containers sc
          ON sc.projection_id = p.projection_id
        INNER JOIN v_catalog.projections pj
          ON pj.projection_id = p.projection_id
    WHERE
        p.statement_id=:stmtid AND
        p.transaction_id=:trxid AND
        pj.is_segmented IS false
    ORDER BY 1, 2, 3
    ;
    \qecho Please note:
    \qecho DVC = Delete Vector Count

    -- ------------------------------------------------------------------------
    -- Query execution profile counters extraction
    -- ------------------------------------------------------------------------
    \echo '    Step 17: Query execution profile counters extraction in CSV format'
    \qecho >>> Step 17: Query execution profile counters extraction in CSV format
    \pset format unaligned
    \pset fieldsep ','
    SELECT
        node_name, 
        operator_name, 
        path_id,
        baseplan_id, 
        localplan_id,
        operator_id, 
        activity_id,
        counter_name, 
        counter_tag, 
        counter_value 
    FROM 
        v_monitor.execution_engine_profiles 
    WHERE 
        transaction_id = :trxid AND 
        statement_id = :stmtid
    ORDER BY 
        node_name, 
        baseplan_id;
    ;
    \pset format aligned
    \pset fieldsep '|'

    -- ------------------------------------------------------------------------
    -- Getting Vertica non-default configuration parameters
    -- ------------------------------------------------------------------------
    \pset expanded
    \echo '    Step 18: Getting Vertica non-default configuration parameters'
    \qecho >>> Step 18: Getting Vertica non-default configuration parameters
    SELECT
        parameter_name, 
        current_value, 
        default_value, 
        description
    FROM 
        v_monitor.configuration_parameters 
    WHERE
        parameter_name !~~ 'SSL%' AND
        current_value <> default_value
    ORDER BY 
        parameter_name;
    ;
    \pset expanded

    -- ------------------------------------------------------------------------
    -- Getting RP configuration
    -- ------------------------------------------------------------------------
    \pset expanded
    \echo '    Step 19: Getting RP configuration'
    \qecho >>> Step 19: Getting RP configuration
    SELECT
        *
    FROM
        v_catalog.resource_pools
    ;
    \pset expanded

    -- ------------------------------------------------------------------------
    -- Getting Cluster configuration
    -- ------------------------------------------------------------------------
    \pset expanded
    \echo '    Step 20: Getting Cluster configuration'
    \qecho >>> Step 20: Getting Cluster configuration
    SELECT
        *
    FROM
        v_monitor.host_resources
    ;

    -- ------------------------------------------------------------------------
    -- Getting Projection Definition
    -- ------------------------------------------------------------------------
    \echo '    Step 21: Getting Projection Definition and Statistics'
    \qecho >>> Step 21: Getting Projection Definition and Statistics
EOF

while read p; do vsql -AtXqn -f - <<-IOF
    SELECT export_objects('','${p}') ;
    \pset format aligned
    \pset null '(null)'
    \pset border 1
    \pset t
    SELECT
        '${p}' AS table,
        projection_name AS projection, 
        projection_column_name AS column, 
        encoding_type, 
        statistics_type, 
        statistics_updated_timestamp AS last_updated 
    FROM
        v_catalog.projection_columns 
    WHERE
        table_schema = SPLIT_PARTB('${p}', '.', 1) AND
        table_name = SPLIT_PARTB('${p}', '.', 2)
    ;
IOF
done >> ${OUT} < <( cat <<-EOF | vsql -A -t -X -q -f -
    \set trxid $TID
    \set stmtid $SID
    SELECT
        DISTINCT ( p.anchor_table_schema || '.' || p.anchor_table_name )
    FROM
        v_monitor.projection_usage p
    WHERE 
        statement_id=:stmtid AND
        transaction_id=:trxid
    ;
EOF
)

#---------------------------------------------------------------------------
# GZIP output file & exit
#---------------------------------------------------------------------------
test ${GZP} -eq 1 && { echo "[qprof] gzipping ${OUT}" ; gzip ${OUT} ; }
exit 0
