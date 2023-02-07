WITH ag_data AS (
    SELECT  kandang, 
            chickin_date AS cycle_start_date,
            cut_off_date, umur AS age, 
            populasi_awal, populasi_akhir, 
            akumulasi_deplesi, abw AS bw,
            total_hpp, hpp_per_ekor, 
            hpp_per_kg, rata_rata_umur, 
            umur_rata_rata_calc,
            CAST(abw AS DECIMAL)*(populasi_akhir+akumulasi_panen)/1000 AS total_BW, 
            total_cummulative_consumption_feed,
            akumulasi_panen - (lag(akumulasi_panen,1) OVER (PARTITIONBY kandang,chickin_date ORDER BY umur)) AS panen_daily, 
            populasi_akhir*umur AS umur_hidup,
            akumulasi_panen, 
            CASE    WHEN CAST(abw AS DECIMAL)*(populasi_akhir+akumulasi_panen)/1000=0 THEN 0 
                    ELSE total_cummulative_consumption_feed/(CAST(abw AS DECIMAL)*(populasi_akhir+akumulasi_panen)/1000)
                    END AS fcr_calc, 
            fcr, 
            total_deplesi,
            CASE    WHEN panen_daily=0 THEN 0 
                    ELSE umur * panen_daily 
                    END AS umur_tangkap_daily,
            ip AS ip_agrinis

    FROM upstream_ops.agrinis_raw
    WHERE   1=1
            [[AND chickin_date>= {{cycle_start_date_f}}]] [[AND chickin_date<= {{cycle_start_date_t}}]]
),

ag_data_2 AS (
    SELECT  *, 
            SUM(umur_tangkap_daily) OVER (PARTITIONBY kandang,cycle_start_date ORDER BY age) AS umur_tangkap_cum
    FROM ag_data
),

ag_data_3 AS (
    SELECT  *,
        --  CASE WHEN akumulasi_panen > 0 THEN CAST(umur_tangkap_cum AS DECIMAL)/akumulasi_panen ELSE age END AS umur_tangkap_1,
        --  (CAST(umur_tangkap_cum AS DECIMAL)+umur_hidup)/(akumulasi_panen+populasi_akhir) AS umur_tangkap_2,
            CASE WHEN panen_daily > 0 THEN AVG(age) FILTER(WHERE panen_daily>0) OVER(PARTITIONBY kandang,cycle_start_date ORDER BY age) 
            ELSE age 
            END AS umur_tangkap_3
    FROM ag_data_2
),

ag_data_4 AS (
    SELECT  ag3.kandang, 
            ag3.cycle_start_date, 
            ag3.cut_off_date, 
            ag3.age,  
            ag3.bw, ag3.populasi_awal, 
            ag3.populasi_akhir,
            ag3.hpp_per_kg, 
            ag3.hpp_per_ekor,
            ag3.total_hpp, 
            ag3.akumulasi_deplesi, 
            ag3.total_cummulative_consumption_feed,
            ag3.bw - LAG(ag3.bw) OVER (PARTITIONBY ag3.kandang, ag3.cycle_start_date ORDER BY age ) AS adg, 
            ag3.rata_rata_umur, 
            ag3.umur_rata_rata_calc,
            ag3.fcr, 
            ag3.fcr_calc, 
            ag3.total_deplesi, 
            ag3.ip_agrinis, 
            md.PPL, 
            md."Mitra Manager", 
            md."Area Manager",
        --  CASE WHEN fcr_calc = 0 THEN 0 ELSE (100-total_deplesi) * BW * 0.1 / (fcr_calc * umur_tangkap_1) END AS ip_calc_1,
        --  CASE WHEN fcr_calc = 0 THEN 0 ELSE (100-total_deplesi) * BW * 0.1 / (fcr_calc * umur_tangkap_2) END AS ip_calc_2,
            CASE    WHEN fcr_calc = 0 THEN 0 
                    ELSE (100-total_deplesi) * BW * 0.1 / (fcr_calc * umur_tangkap_3) 
                    END AS ip_
    FROM ag_data_3 ag3
    
    INNER JOIN (
        SELECT  kandang,
                ppl, 
                "Mitra Manager", 
                "Area Manager"
        FROM upstream_ops.master_data
        WHERE {{PPL}} AND {{mitra_manager}} AND {{area_manager}} AND {{kemitraan}}
        
        ) AS md

    ON ag3.kandang = md.kandang
    ORDER BY    ag3.kandang,
                cycle_start_date, 
                age
),

new_table AS (
    SELECT  ag4.kandang, 
            ag4.cycle_start_date, 
            pmd."Siklus ke-" AS periode, 
            ag4.cut_off_date, 
            ag4.age,  
            ag4.bw, 
            ag4.adg, 
            ag4.fcr, 
            ag4.fcr_calc, 
            ag4.total_deplesi, 
            ag4.ip_agrinis, 
            ag4.PPL, 
            ag4."Mitra Manager", 
            ag4."Area Manager", 
            ag4.ip_
    FROM ag_data_4 ag4
    INNER JOIN (
        SELECT  * 
        FROM upstream_ops.performance_monitoring_daily
        WHERE {{kandang}} AND {{periode}}
        ) pmd

    ON ag4.kandang = pmd."Nama kandang" AND ag4.cycle_start_date = pmd."Cycle start date" AND ag4.age = pmd.age
),

pm_data AS (
    SELECT  ag4.kandang, 
            ag4.cycle_start_date, 
            pmd."Siklus ke-" AS periode, 
            ag4.cut_off_date, 
            ag4.age, 
            ag4.populasi_awal, 
            ag4.populasi_akhir, 
            ag4.bw, 
            ag4.adg, 
            ag4.total_cummulative_consumption_feed,
            ag4.fcr, 
            ag4.fcr_calc,
            ag4.akumulasi_deplesi, 
            ag4.akumulasi_deplesi - LAG(akumulasi_deplesi) OVER (PARTITIONBY ag4.kandang, 
            ag4.cycle_start_date) AS deplesi_harian, 
            ag4.rata_rata_umur, 
            ag4.umur_rata_rata_calc,
            ag4.total_deplesi, 
            ag4.ip_agrinis, 
            ag4.PPL, 
            ag4."Mitra Manager", 
            ag4."Area Manager", 
            ag4.ip_, 
            ag4.hpp_per_kg, 
            ag4.hpp_per_ekor, 
            ag4.total_hpp
    FROM ag_data_4 ag4
    INNER JOIN (
        SELECT *
        FROM upstream_ops.performance_monitoring_daily
        WHERE {{kandang}} AND {{periode}}
        ) pmd
    ON ag4.kandang = pmd."Nama kandang" AND ag4.cycle_start_date = pmd."Cycle start date" AND ag4.age = pmd.age
),

pitik_sales AS (
    SELECT  tanggal,
            kandang AS kandang_ps,
            SUM(actual_bw) actual_bw, 
            SUM(total_kg_sales), 
            SUM(total_sales)/SUM(total_kg_sales) AS harga_per_kg, 
            SUM(total_kg_sales) AS total_kg_sales,
            SUM(total_ekor_sales) AS total_ekor_ayam, 
            SUM(total_sales) AS total_sales
    FROM upstream_ops.pitik_sales
    WHERE total_sales > 0 OR total_kg_sales > 0
    GROUP BY kandang, tanggal
    ORDER BY kandang, tanggal
),


all_data AS (
    SELECT *
    FROM pm_data pm
    LEFT JOIN pitik_sales ps
    ON pm.kandang = ps.kandang_ps AND pm.cut_off_date = ps.tanggal
), 

t1 AS (
    SELECT  kandang, 
            fcr, 
            fcr_calc,
            rata_rata_umur, 
            umur_rata_rata_calc,
            cycle_start_date,
            periode,
            bw,
            total_cummulative_consumption_feed,
            populasi_akhir*bw*1.0/1000 AS tonase_sisa,
            age,
            cut_off_date,
            populasi_awal,
            populasi_akhir,
            akumulasi_deplesi AS deplesi_akumulasi,
            deplesi_harian,
            tanggal,
            CASE    WHEN actual_bw IS NULL THEN 0 
                    ELSE actual_bw 
                    END AS actual_bw,
            CASE    WHEN total_kg_sales IS NULL THEN 0 
                    ELSE total_kg_sales 
                    END AS tonase_panen,
            CASE    WHEN total_ekor_ayam IS NULL THEN 0 
                    ELSE total_ekor_ayam 
                    END AS total_ekor_ayam
    FROM all_data
),


t2 AS (
    SELECT  *,
            SUM(tonase_panen) OVER (PARTITIONBY kandang, cycle_start_date ORDER BY age) AS akumulasi_tonase_panen,
            age*total_ekor_ayam AS umur_x_pop_panen,
            age*populasi_akhir AS umur_x_pop_hidup,
            populasi_awal - deplesi_akumulasi AS total_ayam_hidup
     FROM t1   
),

t3 AS (
    SELECT  *,
            akumulasi_tonase_panen+tonase_sisa AS total_tonase_kg,
            SUM(umur_x_pop_panen) OVER (PARTITIONBY kandang, cycle_start_date ORDER BY age) AS akumulasi_umur_x_pop_panen
    FROM t2
),

t4 AS (
    SELECT  *, 
            (populasi_awal-deplesi_akumulasi)*1.0/populasi_awal*1.0 AS percent_ayam_hidup,
            (total_tonase_kg/total_ayam_hidup)*1000 AS rerata_bw_termasuk_panen,
            total_cummulative_consumption_feed/total_tonase_kg AS fcr_new,
            (akumulasi_umur_x_pop_panen+umur_x_pop_hidup)/total_ayam_hidup AS rerata_umur
    FROM t3
    WHERE bw > 0 AND populasi_awal>0 AND total_tonase_kg > 0
),

ip AS (
    SELECT  *,
    CASE    WHEN fcr_new=0 THEN 0 
            ELSE ((percent_ayam_hidup*rerata_bw_termasuk_panen)/(fcr_new*rerata_umur))*10 
            END AS ip_valid
FROM t4
),

LEFT_data AS (
    SELECT  kandang, 
            cycle_start_date, 
            periode, cut_off_date, 
            t1.age, 
            t1.bw AS bw_actual, 
            std.bw*1000 AS bw_standard, 
            t1.adg AS adg_actual, 
            std.adg*1000 AS adg_standard,
            t1.fcr AS fcr_actual, 
            std.fcr AS fcr_standard, 
            t1.total_deplesi AS deplesi_actual, 
            std.mortality*1.0*100 AS mortality_standard, 
            ppl, 
            "Mitra Manager", 
            "Area Manager", 
            ip_, 
            ip_agrinis
    FROM (
        SELECT  kandang, 
                cycle_start_date, 
                periode, 
                cut_off_date, 
                age, 
                bw, 
                adg, 
                fcr, 
                total_deplesi, 
                ppl, 
                "Mitra Manager", 
                "Area Manager", 
                ip_, 
                ip_agrinis 
        FROM new_table) t1
        LEFT JOIN (
            SELECT  * 
            FROM upstream_ops.standard
            ) std
        ON t1.age = std.age
        ORDER BY    kandang, 
                    cycle_start_date, 
                    age
)

SELECT  ld.kandang, 
        ld.cycle_start_date, 
        ld.periode, 
        ld.cut_off_date, 
        ld.age, ld.bw_actual, 
        ld.bw_standard, 
        ld.adg_actual, 
        ld.adg_standard,
        ld.fcr_actual, 
        ld.fcr_standard, 
        ld.deplesi_actual, 
        ld.mortality_standard, 
        ld.ppl, 
        ld."Mitra Manager", 
        ld."Area Manager", 
        ipx.ip_valid AS ip 
FROM (
    SELECT * 
    FROM LEFT_data
    ) ld
LEFT JOIN (
    SELECT * 
    FROM ip
    ) ipx
ON ld.kandang = ipx.kandang AND ld.cycle_start_date = ipx.cycle_start_date AND ld.age = ipx.age


