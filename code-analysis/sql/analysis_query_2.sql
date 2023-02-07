WITH wn AS (
    SELECT  kandang, 
            chickin_date, 
            umur, 
            cut_off_date,
            populasi_awal, 
            populasi_akhir, 
            cat_umur
    FROM (
        SELECT  BTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(kandang, 'AHMAD FAUZI A (GOMBONG)', 'AHMAD FAUZI ( Gombong )'), 'FAWAID SYAMSUL', 'FAWAID SYAMSUL ARIFIN'), 'MUHAMAD GHOZALI B', 'MUHAMMAD GHOZALI B'),'MUHAMAD GHOZALI C','MUHAMMAD GHOZALI C'),
                    'HAMDAN M. NAFI A', 'HAMDAN M NAFI A'),'HAMDAN M. NAFI B','HAMDAN M NAFI B'),'CONDRO WIRYONO','CONDRO WIRYONO POERWANTO'), 'MUHAMMAD DARYL AVISENA','MUHAMMAD DARYL AVISENA SULIAN'),'SOBIRIN A 004)','SOBIRIN A'),'TRIE MULYANA','TRIE MULYANA A'),'NURSHIRWAN HARUN AL RASYID A','NURSHIRWAN HARUN AL RASYID'),'DEWI NURGAIRAH ( NZF)','DEWI NURGAIRAH (NZF)'),'BUDI PRIYONO ( CITAPEN FARM 2','BUDI PRIYONO ( CITAPEN FARM 2)'),
                    'YUDHA OCTAVIANTO A', 'YUDHA OKTAVIANTO A'),'YUDHA OCTAVIANTO B','YUDHA OKTAVIANTO B'),'H DARMAJI A','DARMAJI A'),'H DARMAJI B','DARMAJI B'),'FENDI WAHYUDI','FENDI WAHYUDI A'),'DRS NURI FATHUROHMAN, M.PD','DRS NURI FATHUROHMAN'),'DONI EKO','DONI EKO ARIFIANTO'),'YUDHA OCTAVIANTO C','YUDHA OKTAVIANTO C'),'YUNUS','MUHAMAD YUNUS'),'TRIE MULYANA A B','TRIE MULYANA B'),'GIGIH SETIONO','RISTIANA SARI'),
                    'NOR ROHMAN HARIYANTO','NUR ROHMAN HARIYANTO'),'SITI FATIMAH TLU','SITI FATIMAH'),'KAMILUR ROSYAD A','KAMILUR ROSYAD'),'ADI MUHAMAD MUHSIDI','ADI MUHAMAD MUSHIDI'),'YOGAIR FISKA','YOGAIR FISKA ASHILMI'),'RUSYAD FIRDAUSI', 'RUSYAD FIRDAUSI A'),'MUHAMAD GHOZALI','MUHAMMAD GHOZALI A'),' ') AS kandang,
                chickin_date, 
                umur, 
                cut_off_date,
                populasi_awal, 
                populasi_akhir,
                CASE    WHEN umur <= 7 THEN 'w1'
                        WHEN umur > 7 AND umur <= 14 THEN 'w2'
                        WHEN umur > 14 AND umur <= 21 THEN 'w3'
                        WHEN umur > 21 AND umur <= 28 THEN 'w4'
                        WHEN umur > 28 AND umur <= 35 THEN 'w5'
                        WHEN umur > 35 THEN 'w6'
                        END AS cat_umur,
                ROW_NUMBER() OVER(PARTITION BY kandang, chickin_date ORDER BY kandang,chickin_date ASC,umur DESC) AS umur_rank 
        FROM upstream_ops.agrinis_raw
        ) t1 
        WHERE   umur_rank <= 1 AND populasi_akhir > 0 
                [[AND cut_off_date >= current_date - interval '{{last_n_days}}' day]] 
        ORDER BY    kandang,
                    chickin_date,
                    umur
),

t2 AS (
    SELECT  *, 
            CASE WHEN cat_umur = 'w1' THEN populasi_akhir ELSE 0 END AS "W1 (0-7)",
            CASE WHEN cat_umur = 'w2' THEN populasi_akhir ELSE 0 END AS "W2 (7-14)",
            CASE WHEN cat_umur = 'w3' THEN populasi_akhir ELSE 0 END AS "W3 (14-21)",
            CASE WHEN cat_umur = 'w4' THEN populasi_akhir ELSE 0 END AS "W4 (21-28)" FROM wn
),

ppl_t AS (
    SELECT distinct ppl, 
                    "Kantor Cabang" AS Kantor_cabang, 
                    area 
    FROM upstream_ops.mapper_unit
),

t3 AS (
    SELECT * FROM t2
    LEFT JOIN (
        SELECT  BTRIM(kandang,' ') AS kandang_md, 
                wilayah AS wilayah, 
                BTRIM(REPLACE("PPL (A)",'IMAM ROFI''L','IMAM ROFI''L'), ' ') AS ppl_md, 
                REPLACE(REPLACE(REPLACE("Mitra Manager", 'HINDRO', 'INDRO'), 'ZAENUL', 'ZAINUL'), 'FATEKHUL', 'F. ASYHAR') AS mm_md 
        FROM upstream_ops.master_data
        ) md 
    ON UPPER(t2.kandang) = UPPER(md.kandang_md)
    LEFT JOIN (
        SELECT  BTRIM(REPLACE(ppl,'Imam  Rofi''l','IMAM ROFI''L'),' ') AS ppl, 
                Kantor_cabang, area 
        FROM ppl_t
        ) mu
    ON BTRIM(UPPER(md.ppl_md),' ') = BTRIM(UPPER(mu.ppl),' ')
)

SELECT  area AS "Province", 
        Kantor_cabang AS "Unit", 
        mm_md AS "Mitra Manager", 
        SUM(t3."W1 (0-7)") AS "W1 (0-7)", 
        SUM(t3."W2 (7-14)") AS "W2 (7-14)", 
        SUM(t3."W3 (14-21)") AS "W3 (14-21)", 
        SUM(t3."W4 (21-28)") AS "W4 (21-28)",
        SUM(t3."W1 (0-7)") + SUM(t3."W2 (7-14)") + SUM(t3."W3 (14-21)") + SUM(t3."W4 (21-28)") AS "Grand Total"
FROM t3
GROUP BY "Province","Unit", "Mitra Manager"
ORDER BY "Province","Unit", "Mitra Manager"


