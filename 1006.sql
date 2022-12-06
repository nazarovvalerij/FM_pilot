CREATE OR REPLACE PACKAGE BODY SB_FCT.pkg_pilot_1006
is


  c_schema   varchar(20) := upper(sys_context('USERENV', 'CURRENT_USER')) || '.';
  c_pkg_name varchar(50) := c_schema || 'pkg_pilot_1006'; --Записываем название пакета
  c_pilot_num number := regexp_replace(c_pkg_name,'[^[:digit:]]');
  

procedure p_run (
    i_start_dt date default null,
    i_end_dt date default null,
    i_debug_n number default 0 --1 - Тестовый запуск
  ) is
    --v_start_date  date;
    --v_end_date         date;
  begin
    
    sb_utl.pkg_log.p_start_pkg(c_pkg_name
                              ,'Назаров В.Е.'
                              ,'Назаров В.Е.');
  
    
    p_pilot_1006(i_start_dt, i_end_dt, i_debug_n); --Основная процедура
    
    sb_utl.pkg_log.p_end_pkg(c_pkg_name); 
  exception
    --Обработка ошибок*
    when others then
      sb_utl.pkg_log.p_add_log(c_pkg_name
                              ,'NULL'
                              ,'Ошибка : ' || sqlerrm || ' >> ' ||
                               dbms_utility.format_error_backtrace
                              ,'error');
      rollback;
      sb_utl.pkg_log.p_end_pkg(c_pkg_name);
      raise;
  end p_run;
 
  -----------------------------------p_pilot_1006---------------------------
    
  procedure p_pilot_1006 (
    i_start_dt date default null
   ,i_end_dt date default null
   ,i_debug_n number default 0
  ) is
    c_p_name varchar(150) := c_pkg_name || '.' || 'p_pilot_1006';  --название процедуры
    v_start_dt date;
    v_end_dt date;
    --v_start_proc_dt date; --Системное время, для начала отсчёта
    --v_max_dt number;
    v_new_bdt date; --Новая Бизнес-дата загрузки
    v_old_bdt date; --Старая
    v_ins_BUF_FCT_OPERATIONS_NUM number; --Для дебага. Количество вставленных строк в таблицу BUF_FCT_OPERATIONS
    v_start_date  date;
  begin
    v_start_date := date'2021-08-01';
    sb_utl.pkg_log.p_start_proc(c_pkg_name, c_p_name);
    
    --v_start_proc_dt := sysdate;
    
    v_start_dt := i_start_dt;
    v_end_dt := i_end_dt;
    
    if (v_end_dt is null)
    then
        v_end_dt := trunc(sysdate);
    end if;
    
    if (v_start_dt is null)
    then
        v_old_bdt := sb_utl.pkg_business_date.f_get('ASM_SANDBOX', 'FCT_OPERATIONS', 'pkg_pilot_1006');
        v_start_dt := v_old_bdt - 4;
    end if;
    
    if (v_start_dt is null)
    then
        v_start_dt := v_end_dt - 5;
    end if;
    
    -----------------------------------ТЕЛО----------------------------------------

   	sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'truncate from SB_BUF.PILOT_1006',
                                    i_dt_start => v_start_dt, i_dt_finish => v_end_dt);
    
    sb_utl.pkg_utl.p_truncate(i_pkg_name => c_pkg_name, i_p_name => c_p_name, i_table_full_name => 'SB_BUF.PILOT_1006',i_event_level => 'no_log');
  
    
    sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'end truncate from SB_BUF.PILOT_1006',
            i_cnt_changes => sql%rowcount, i_dt_start => v_start_dt, i_dt_finish => v_end_dt);
    
    commit;

    
    sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'insert SB_BUF.PILOT_1006 <- select ASM_SANDBOX.FCT_OPERATIONS_PREMIER',
                                    i_dt_start => v_start_dt, i_dt_finish => v_end_dt);

                                   
    insert into SB_BUF.PILOT_1006
    (
    SOURCE_SYSTEM_CODE, 
	ID_VDMRB, 
	PRODUCT_ID, 
	PRODUCT_NAME, 
	OPERATION_TYPE_ID, 
	OPERATION_NAME, 
	AMOUNT_RUR, 
	PRODUCT_ITEM_ID, 
	CHNL, 
	employee_fio,
	TAB_NUM, 
	CLIENT_FIO, 
	REPORT_DATE, 
	OPERATION_DATE, 
	OPERATION_DATE_SRC,
	CLIENT_EPK,
	is_fraud,
	in_motiv,
	  cnt,
	  KD,
	  id_vdmrb_uniq
	) 
 
select * from (
select --+parallel(8) 
   distinct
                                
    x.SOURCE_SYSTEM_CODE, 
	CASE 	WHEN (SOURCE_SYSTEM_CODE = 'ASBS' AND OPERATION_TYPE_ID IN ('Оформление ИПП\КПП')) THEN x.PRODUCT_ITEM_ID 
			ELSE x.ID_VDMRB END AS ID_VDMRB, 
	x.PRODUCT_ID,
	x.PRODUCT_NAME, 
	x.OPERATION_TYPE_ID, 
	x.OPERATION_NAME, 
	x.AMOUNT_RUR, 
	CASE 	WHEN (SOURCE_SYSTEM_CODE = 'PIFIYA' AND OPERATION_TYPE_ID IN ('ПИФ','ЗПИФ','ИИС','ДУ')) THEN x.AGREEMENT_NUM 
			ELSE x.PRODUCT_ITEM_ID END AS PRODUCT_ITEM_ID,  
	'ВСП' AS CHNL, 
	x.EMPLOYEE_FIO,
	null as TAB_NUM,--x.TAB_NUM, 
	x.CLIENT_FIO, 
	x.REPORT_DATE, 
	x.OPERATION_DATE, 
	CASE WHEN  (SOURCE_SYSTEM_CODE = 'PIFIYA' AND OPERATION_TYPE_ID IN ('ИИС', 'ДУ')) THEN x.CREATE_DATE 
	ELSE (
			CASE 	WHEN x.CREATE_DATE>=--date'2021-09-01'
   										x.REPORT_DATE THEN x.CREATE_DATE --13-10-2021
					ELSE x.operation_date END
		) end AS OPERATION_DATE_SRC, --test
	x.CLIENT_EPK AS CLIENT_EPK,
	'0' is_fraud,
	'0' in_motiv,
	CASE 	WHEN (SOURCE_SYSTEM_CODE = 'PIFIYA' AND OPERATION_TYPE_ID IN ('ИИС', 'ДУ')) THEN row_number()over(partition by OPERATION_TYPE_ID, CLIENT_EPK,product_item_iD order by OPERATION_DATE asc )
  			ELSE row_number()over(partition by PRODUCT_ITEM_ID, SOURCE_SYSTEM_CODE order by OPERATION_DATE asc ) --13.10.2021
  			END AS cnt,
  	x.COMM AS kd,
  	sys.dbms_crypto.hash(utl_i18n.string_to_raw(x.SOURCE_SYSTEM_CODE||ID_VDMRB||AMOUNT_RUR||PRODUCT_ITEM_ID||to_char(OPERATION_DATE)||CLIENT_EPK, 'AL32UTF8'), 2) AS id_vdmrb_uniq --нужно захэшировать 18-10-2021 Назаров
from ASM_SANDBOX.FCT_OPERATIONS_PREMIER x 
WHERE 1=1
	                                             

	and	(
	--ИПП
	(SOURCE_SYSTEM_CODE = 'ASBS'
	     AND OPERATION_TYPE_ID IN ('Оформление ИПП\КПП'))
	or
	--ИСЖ
	(SOURCE_SYSTEM_CODE = 'ASBS'
	    AND OPERATION_TYPE_ID IN ('ИСЖ'))
	or
	--НСЖ
	(SOURCE_SYSTEM_CODE = 'ASBS'
	    AND OPERATION_TYPE_ID IN ('НСЖ'))
	or
	--ПИФ
	(SOURCE_SYSTEM_CODE = 'PIFIYA'
	         AND OPERATION_TYPE_ID IN ('ПИФ','ЗПИФ'))
        OR
        -- Открытие брокерского счёта
        x.CALC_PRODUCT_ID ='0008.0011.0009' 
		
	or
	--ДУ/ИИС
	(SOURCE_SYSTEM_CODE = 'PIFIYA'
         AND OPERATION_TYPE_ID IN ('ИИС', 'ДУ'))
	)
	AND lower(x.OPERATION_NAME) NOT LIKE '%комиссия%'
	AND x.CLIENT_EPK <> '-1'	
  )

	where 1=1
  and  report_date between v_start_dt AND v_end_dt
  ;

   
    sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'inserted into SB_BUF.PILOT_1006',
                i_cnt_changes => sql%rowcount, i_dt_start => v_start_dt, i_dt_finish => v_end_dt);
    
    commit;

   




 
   
--Облигации сбербанка 3 и 5 лет 
  insert into SB_BUF.PILOT_1006
    (
    SOURCE_SYSTEM_CODE, 
	ID_VDMRB, 
	PRODUCT_ID, 
	PRODUCT_NAME, 
	OPERATION_TYPE_ID, 
	OPERATION_NAME, 
	AMOUNT_RUR, 
	PRODUCT_ITEM_ID, 
	CHNL, 
	employee_fio,
	TAB_NUM, 
	CLIENT_FIO, 
	REPORT_DATE, 
	OPERATION_DATE, 
	OPERATION_DATE_SRC, 
	CLIENT_EPK,
	is_fraud,
	in_motiv,
	  cnt,
	  KD,
	  id_vdmrb_uniq
	)  
SELECT 
	SOURCE_SYSTEM_CODE,
	ID_VDMRB, 
	0 AS PRODUCT_ID,
	CASE 	WHEN srok = '3' THEN 'Инвестиционные облигации Сбербанка на 3 года'
			WHEN srok = '5' THEN 'Инвестиционные облигации Сбербанка на 5 лет' 
			ELSE 'Инвестиционные облигации Сбербанка' END AS PRODUCT_NAME, 
	'BROKER_COMMISSION' AS OPERATION_TYPE_ID,
	OPERATION_NAME, 
	AMOUNT_RUR, 
	PRODUCT_ITEM_ID,
	'ВСП' AS CHNL,
	NULL AS employee_fio,
	NULL AS TAB_NUM,
	CLIENT_FIO, 
	REPORT_DATE, 
	OPERATION_DATE, 
	OPERATION_DATE_SRC, 
	CLIENT_EPK,
	'0' is_fraud,
	'0' in_motiv,
	row_number()over(partition by id_vdmrb, OPERATION_DATE order by OPERATION_DATE asc ) AS cnt,
	kd,
	sys.dbms_crypto.hash(utl_i18n.string_to_raw(SOURCE_SYSTEM_CODE||ID_VDMRB||AMOUNT_RUR||PRODUCT_ITEM_ID||to_char(OPERATION_DATE)||CLIENT_EPK, 'AL32UTF8'), 2) AS id_vdmrb_uniq
from(	  
	SELECT --+parallel(4)
		'CBDBO' AS 	SOURCE_SYSTEM_CODE,
		K.AGRMNT_CODE AS ID_VDMRB,--CLIENT_ID,
		asd.vidcb ||'_'|| asd.isin AS OPERATION_NAME,
		CASE WHEN K.CURRENCY = 'RUR' THEN K.AMOUNT_LCL
		                                 ELSE (K.AMOUNT_LCL * K.RATE)
		                           END AS  AMOUNT_RUR,
		NULL AS  PRODUCT_ITEM_ID,	                           
		client_fio AS CLIENT_FIO,
		TRUNC(CAST(K.REPORT_DT AS DATE), 'MM') AS report_date,
		CAST(K.REPORT_DT AS DATE) AS OPERATION_DATE,
		CAST(K.REPORT_DT AS DATE) AS operation_date_src,
		c.CLIENT_EPK AS CLIENT_EPK,
		srok,
		k.RATE*k.BANKCOMMISSION*0.5 AS KD
	FROM ASM_SANDBOX.fct_bo_je_details K
	JOIN ASM_SANDBOX.FCT_BROKER_CLIENT_EPK C
	    ON C.CLIENT_ID = K.AGRMNT_CODE
	join (select distinct asd.securityid, asd.isin,  vidcb, asd.typecb, asd.execdate 
	                     ,round((asd.dateend - asd.primedispositiondate)/365) as srok
	     from ASM_SANDBOX.X_BO_DIC_SECURITY_LIST asd
	    ) asd
		 on K.Security_Id_New = asd.securityid
		 and asd.vidcb = 'Облигации'
		 and asd.typecb = 'БСО'
		 and asd.execdate >= v_start_dt
	 WHERE trunc(K.REPORT_DT) BETWEEN v_start_dt AND v_end_dt
		 and K.Market_Type =0
		 and K.Contragent = 'Сбербанк КИБ'
		 and K.Isrepo = 0
		 and K.Trans_Type = 1001
		 AND c.CLIENT_EPK != '-1'
		
		
	);	 
   
    sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'inserted ИОС into SB_BUF.PILOT_1006',
                i_cnt_changes => sql%rowcount, i_dt_start => v_start_dt, i_dt_finish => v_end_dt);
               
COMMIT;                  



   --Евробонды
  insert into SB_BUF.PILOT_1006
    (
    SOURCE_SYSTEM_CODE, 
	ID_VDMRB, 
	PRODUCT_ID, 
	PRODUCT_NAME, 
	OPERATION_TYPE_ID, 
	OPERATION_NAME, 
	AMOUNT_RUR, 
	PRODUCT_ITEM_ID, 
	CHNL, 
	employee_fio,
	TAB_NUM, 
	CLIENT_FIO, 
	REPORT_DATE, 
	OPERATION_DATE, 
	OPERATION_DATE_SRC, 
	CLIENT_EPK,
	is_fraud,
	in_motiv,
	  cnt,
	  KD,
	  id_vdmrb_uniq,
	AMOUNT_CURR,
	CURRENCY
	)  
Select      
    SOURCE_SYSTEM_CODE,
	ID_VDMRB, 
	0 AS PRODUCT_ID,
	'Еврооблигации' AS PRODUCT_NAME, 
	'BROKER_COMMISSION' AS OPERATION_TYPE_ID,
	OPERATION_NAME, 
	AMOUNT_RUR, 
	PRODUCT_ITEM_ID,
	'ВСП' AS CHNL,
	NULL AS employee_fio,
	NULL AS TAB_NUM,
	CLIENT_FIO, 
	REPORT_DATE, 
	OPERATION_DATE, 
	OPERATION_DATE_SRC, 
	CLIENT_EPK,
	'0' is_fraud,
	'0' in_motiv,
	row_number()over(partition by id_vdmrb, OPERATION_DATE order by OPERATION_DATE asc ) AS cnt,
	kd,
	sys.dbms_crypto.hash(utl_i18n.string_to_raw(SOURCE_SYSTEM_CODE||ID_VDMRB||AMOUNT_RUR||PRODUCT_ITEM_ID||to_char(OPERATION_DATE)||CLIENT_EPK, 'AL32UTF8'), 2) AS id_vdmrb_uniq,
	AMOUNT_CURR,
	CURRENCY
from(	  
	SELECT --+parallel(4) 
		'CBDBO' AS 	SOURCE_SYSTEM_CODE,
		K.AGRMNT_CODE AS ID_VDMRB,--CLIENT_ID,
		asd.vidcb ||'_'|| asd.isin AS OPERATION_NAME,
		CASE WHEN K.CURRENCY = 'RUR' THEN K.AMOUNT_LCL
		                                 ELSE (K.AMOUNT_LCL * K.RATE)
		                           END AS  AMOUNT_RUR,
		NULL AS  PRODUCT_ITEM_ID,	                           
		client_fio AS CLIENT_FIO,
		TRUNC(CAST(K.REPORT_DT AS DATE), 'MM') AS report_date,
		CAST(K.REPORT_DT AS DATE) AS OPERATION_DATE,
		CAST(K.REPORT_DT AS DATE) AS operation_date_src,
		c.CLIENT_EPK AS CLIENT_EPK,
		K.AMOUNT_LCL AS AMOUNT_CURR,
		K.CURRENCY AS CURRENCY,
		k.RATE*k.BANKCOMMISSION*0.5 AS KD
   from ASM_SANDBOX.FCT_BO_JE_DETAILS k
        JOIN ASM_SANDBOX.FCT_BROKER_CLIENT_EPK C
        ON C.CLIENT_ID = k.AGRMNT_CODE
	   join ASM_SANDBOX.X_BO_DIC_SECURITY_LIST asd
	 on K.Security_Id_New = asd.securityid
	 and asd.vidcb = 'Облигации'
   where k.CLASSCODE in ('BQSEBAUK','BQSEBUK', 'BBONDSBERUK','BQEBAUK')
   AND c.CLIENT_EPK != '-1'
   and trunc(k.Report_Dt) between v_start_dt AND v_end_dt
   );
   
    sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'inserted Еврооблигации into SB_BUF.PILOT_1006',
                i_cnt_changes => sql%rowcount, i_dt_start => v_start_dt, i_dt_finish => v_end_dt);
               
COMMIT;       




--убираем премьер- и вип-клиентов из выгрузки

    sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'delete from SB_BUF.PILOT_1006 - select ASM_SANDBOX.EPK_CLNT',
                                    i_dt_start => v_start_dt, i_dt_finish => v_end_dt);
                                   
	DELETE FROM SB_BUF.PILOT_1006 p
	WHERE p.CLIENT_EPK IN (SELECT b.ACTUAL_CLIENT_EPK FROM ASM_SANDBOX.EPK_CLNT b
							WHERE p.OPERATION_DATE BETWEEN b.ACTUAL_FROM_DT AND b.ACTUAL_TO_DT);
						
    sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'end delete from SB_BUF.PILOT_1006',
                i_cnt_changes => sql%rowcount, i_dt_start => v_start_dt, i_dt_finish => v_end_dt);
               
	COMMIT;

--Фрод, если брокерский счёт был открыт ранее
sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'fraud 6 у клиенты был БС SB_BUF.PILOT_1006',
                                i_dt_start => v_start_dt, i_dt_finish => v_end_dt);

merge  --+ parallel (4)
into
SB_BUF.PILOT_1006  a
USING (
	SELECT 
	rowid rw
	FROM sb_buf.PILOT_1006 p2 
	WHERE p2.CLIENT_EPK IN (
							SELECT 
							
							fe.CLIENT_EPK
							
							FROM ASM_SANDBOX.FCT_BROKER_CLIENT_EPK fe
							WHERE fe.CLIENT_EPK IN (SELECT --+parallel(16)
													p.CLIENT_EPK 
													from sb_buf.PILOT_1006 p 
													WHERE 1=1
														AND p.OPERATION_DATE >= v_start_dt
														AND p.OPERATION_NAME = 'Открытие брокерского счета'
														AND p.OPERATION_TYPE_ID = 'BROKER_ACCOUNT'
													)
							AND fe.AGRMNT_OPEN_DT<v_start_dt
							)
		AND p2.OPERATION_TYPE_ID = 'BROKER_ACCOUNT'		
		AND p2.OPERATION_NAME = 'Открытие брокерского счета'
		) b
ON (a.rowid=b.rw)		
WHEN MATCHED THEN UPDATE 
	SET a.IS_FRAUD = 6, a.in_motiv = 0;

  
    sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'end fraud 6 у клиенты был БС SB_BUF.PILOT_1006',
                i_cnt_changes => sql%rowcount, i_dt_start => v_start_dt, i_dt_finish => v_end_dt);

commit;

--Фрод, если на открытый брокерский счёт не было совершено покупки облигаций на сумму >=10000р (или 1000у.е.)

sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'fraud 9 на открытый БС не было покупки 10к+(руб.) или 1к+(у.е.) SB_BUF.PILOT_1006',
                                i_dt_start => v_start_dt, i_dt_finish => v_end_dt);

merge  --+ parallel (4)
into
SB_BUF.PILOT_1006  a
USING (
		SELECT --parallel(16)
		distinct
		p2.rowid AS rw
		FROM 
		sb_buf.PILOT_1006 p 
		JOIN
		sb_buf.PILOT_1006 p2
			ON p.ID_VDMRB = p2.ID_VDMRB 
			AND p2.OPERATION_TYPE_ID = 'BROKER_ACCOUNT'	
			AND p.OPERATION_TYPE_ID = 'BROKER_COMMISSION'
		WHERE 1=1
			AND p2.OPERATION_DATE BETWEEN v_start_dt AND v_end_dt
			AND p.TAB_NUM IS NULL
			AND p2.TAB_NUM IS NULL
			AND (p.AMOUNT_CURR <1000 OR (p.AMOUNT_RUR <10000 AND p.AMOUNT_CURR IS NULL) or p.ID_VDMRB IS null)
		) b
ON (a.rowid=b.rw)		
WHEN MATCHED THEN UPDATE 
	SET a.IS_FRAUD = 9, a.in_motiv = 0;

  
    sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'end fraud 6 у клиенты был БС SB_BUF.PILOT_1006',
                i_cnt_changes => sql%rowcount, i_dt_start => v_start_dt, i_dt_finish => v_end_dt);

commit;
			


--Фрод, если новый открытый брокерский счёт был закрыт до 2го числа следующего за отчетным месяца

sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'fraud 8 БС был закрыт SB_BUF.PILOT_1006',
                                i_dt_start => v_start_dt, i_dt_finish => v_end_dt);

merge  --+ parallel (4)
into
SB_BUF.PILOT_1006  a
USING (
		SELECT --+parallel(16)
		DISTINCT
			x2.rowid AS rw

		FROM ASM_SANDBOX.FCT_BROKER_CLIENT_EPK fe
		JOIN SB_BUF.PILOT_1006 x2
			ON x2.CLIENT_EPK = fe.CLIENT_EPK 
		WHERE 1=1
			AND x2.OPERATION_DATE >= v_start_dt
			AND x2.OPERATION_NAME = 'Открытие брокерского счета'	
			AND x2.TAB_NUM IS null
			AND fe.AGRMNT_OPEN_DT >= v_start_dt
			AND fe.AGRMNT_CLOSE_DT IS NOT NULL
			AND fe.AGRMNT_CLOSE_DT <= last_day(x2.REPORT_DATE)+2
			AND x2.IS_FRAUD = '0'
		) b
ON (a.rowid=b.rw)		
WHEN MATCHED THEN UPDATE 
	SET a.IS_FRAUD = 8, a.in_motiv = 0;

    sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'end fraud 8 БС был закрыт SB_BUF.PILOT_1006',
                i_cnt_changes => sql%rowcount, i_dt_start => v_start_dt, i_dt_finish => v_end_dt);

commit;

--фрод открытия БС сотруднику 30.11.2021

    sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'фрод открытия себе или другому сотруднику сбербанка',
                                    i_dt_start => v_start_dt, i_dt_finish => v_end_dt);

          Merge into SB_BUF.PILOT_1006 op
       using (  select distinct(o.ID_VDMRB), s.epk_id, o.rowid AS rw
				from SB_BUF.PILOT_1006 o
				left join  sb_src.sap_dic_epk_sales s
				on (o.CLIENT_EPK = s.epk_id
					and o.operation_date between s.start_date and s.end_date 
					)
				where o.operation_date between v_start_dt AND v_end_dt
				AND s.EPK_ID IS NOT NULL
				AND o.OPERATION_TYPE_ID = 'BROKER_ACCOUNT') f
       on (op.rowid = f.rw)
       when matched then 
         update set op.is_fraud = 10, op.in_motiv = 0
       ;
      
          sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'END фрод открытия себе или другому сотруднику сбербанка',
                i_cnt_changes => sql%rowcount, i_dt_start => v_start_dt, i_dt_finish => v_end_dt);
      
      COMMIT;
 

    sb_utl.pkg_log.p_add_log(c_pkg_name,
                             c_p_name,
                             'delete from ASM_SANDBOX.BUF_FCT_OPERATIONS',
                             i_cnt_changes                               => sql%rowcount,
                             i_dt_start                                  => v_start_dt,
                             i_dt_finish                                 => v_end_dt);


--Заменяем operation_date на report_date у операций, совершенных в последних числах предыдущего месяца (начала периода). Постоянная история Дату операции заливаем в operation_date_src. Назаров В.Е. 30.09.2021

    sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'update SB_BUF.PILOT_1006 operation_dt < report_dt',
                                    i_dt_start => v_start_dt, i_dt_finish => v_end_dt);

	UPDATE sb_buf.PILOT_1006 p
	SET p.OPERATION_DATE = p.REPORT_DATE
	WHERE p.OPERATION_DATE<p.REPORT_DATE;

    sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'end update SB_BUF.PILOT_1006 operation_dt < report_dt',
                i_cnt_changes => sql%rowcount, i_dt_start => v_start_dt, i_dt_finish => v_end_dt);

	COMMIT;           
               
        

--подставляем product_id в строки, где он null 15-11-2021
    sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'update product_id SB_BUF.PILOT_1006',
                                    i_dt_start => v_start_dt, i_dt_finish => v_end_dt);

    merge  --+ parallel (4)
    into
    SB_BUF.PILOT_1006  a
	USING (
	
	--стали попадаться несколько product_name с разными product_id 
			SELECT * FROM (
							SELECT --+parallel(16) 
							row_number()over(PARTITION BY PRODUCT_NAME ORDER BY PRODUCT_ID asc) AS rn, 
							a.* 
							FROM (
									SELECT distinct
									fop.PRODUCT_ID  , fop.PRODUCT_NAME
									FROM SB_BUF.PILOT_1006 fop 
									WHERE 1=1
									AND fop.PRODUCT_ID IS not NULL 
								) a
						)
			WHERE rn=1
				 ) b
	ON (a.PRODUCT_NAME = b.PRODUCT_NAME )
		WHEN MATCHED THEN UPDATE 
	SET a.PRODUCT_ID = b.PRODUCT_ID;
	
	  sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'end update product_id SB_BUF.PILOT_1006',
                i_cnt_changes => sql%rowcount, i_dt_start => v_start_dt, i_dt_finish => v_end_dt);
  
commit;
 


-- фрод ИПП, ИИС. Не премируется, периодический платеж.(7)
    sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'update fraud 7 ИПП SB_BUF.PILOT_1006',
                                    i_dt_start => v_start_dt, i_dt_finish => v_end_dt);

   update SB_BUF.PILOT_1006 x
       	set is_fraud = 7, in_motiv = 0
     where x.cnt >1 
     and SOURCE_SYSTEM_CODE in ('ASBS','PIFIYA','CBDBO')
	   AND OPERATION_TYPE_ID IN ('Оформление ИПП\КПП','ИИС','ДУ','BROKER_COMMISSION');
     
  sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'end update fraud 7 ИПП SB_BUF.PILOT_1006',
                i_cnt_changes => sql%rowcount, i_dt_start => v_start_dt, i_dt_finish => v_end_dt);
  
commit;


    sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'update fraud 5 сумма еврооблиг SB_BUF.PILOT_1006',
                                    i_dt_start => v_start_dt, i_dt_finish => v_end_dt);

-- фрод на сумму еврооблигаций в день
    update SB_BUF.PILOT_1006 x
       	set is_fraud = 5, in_motiv = 0
     where x.rowid IN (
     					SELECT rw FROM (
						 				SELECT --+parallel(16)
						 					p.rowid AS rw,
											sum(p.AMOUNT_CURR)OVER(PARTITION BY p.CLIENT_EPK,p.OPERATION_DATE) summ,
											p.AMOUNT_CURR ,
											p.CLIENT_EPK,
											p.OPERATION_DATE
											,p.CNT
										FROM  SB_buf.PILOT_1006 p
										where 1=1
										 	--AND p.CNT =1
										 	AND p.OPERATION_TYPE_ID IN ('BROKER_COMMISSION')
										 	AND p.SOURCE_SYSTEM_CODE IN ('CBDBO')
										 	AND p.AMOUNT_CURR IS NOT NULL
										 	AND p.CLIENT_EPK != '-1'
										) a
										WHERE a.summ<1000
											AND a.cnt=1
     					)
     	
       	AND trunc(operation_date) between v_start_dt and v_end_dt 
		;
      
    sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'end update fraud 5 сумма еврооблиг SB_BUF.PILOT_1006',
                i_cnt_changes => sql%rowcount, i_dt_start => v_start_dt, i_dt_finish => v_end_dt);
               
	COMMIT;      

    sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'update fraud 1 сумма ИОС SB_BUF.PILOT_1006',
                                    i_dt_start => v_start_dt, i_dt_finish => v_end_dt);

-- фрод на сумму облигаций сбера в день
    update SB_BUF.PILOT_1006 x
       	set is_fraud = 1, in_motiv = 0
     where x.rowid IN (
     				SELECT rw from(
								
									
									SELECT --+parallel(16)
						 					p.rowid AS rw,
											sum(p.AMOUNT_RUR)OVER(PARTITION BY p.CLIENT_EPK,p.OPERATION_DATE) summ,
											p.AMOUNT_rUR ,
											p.CLIENT_EPK,
											p.OPERATION_DATE
											,p.CNT
										FROM  SB_buf.PILOT_1006 p
										where 1=1
										 	--AND p.CNT =1
										 	AND p.OPERATION_TYPE_ID IN ('BROKER_COMMISSION')
										 	AND p.SOURCE_SYSTEM_CODE IN ('CBDBO')
										 	AND p.AMOUNT_CURR IS NULL
										 	AND p.CLIENT_EPK != '-1'
										) a
										WHERE a.summ<10000
											AND a.cnt=1
									
     					)
       	AND trunc(operation_date) between v_start_dt and v_end_dt 
		;
      
    sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'end update fraud 1 сумма ИОС SB_BUF.PILOT_1006',
                i_cnt_changes => sql%rowcount, i_dt_start => v_start_dt, i_dt_finish => v_end_dt);
               
	COMMIT;      



	sb_utl.pkg_log.p_add_log(c_pkg_name,
	                             c_p_name,
	                             'merge fraud 2 SB_BUF.PILOT_1006',
	                             i_cnt_changes                      => sql%rowcount,
	                             i_dt_start                         => v_start_dt,
	                             i_dt_finish                        => v_end_dt);         


    sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'update fraud 1 ПИФ SB_BUF.PILOT_1006',
                                    i_dt_start => v_start_dt, i_dt_finish => v_end_dt);

-- фрод на сумму ПИФ в день
    update SB_BUF.PILOT_1006 x
       	set is_fraud = 1, in_motiv = 0
     where amount_rur < 14800
     	AND x.OPERATION_TYPE_ID IN ('ПИФ','ЗПИФ')
		AND lower(x.OPERATION_NAME) NOT LIKE '%комиссия%'
		AND lower(x.OPERATION_NAME) NOT LIKE '%от объемов%'
       	--and operation_date between v_start_dt and v_end_dt
       	AND trunc(operation_date) between v_start_dt and v_end_dt 
		;
      
    sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'end update fraud 1 ПИФ SB_BUF.PILOT_1006',
                i_cnt_changes => sql%rowcount, i_dt_start => v_start_dt, i_dt_finish => v_end_dt);
               
	COMMIT;      

	sb_utl.pkg_log.p_add_log(c_pkg_name,
	                             c_p_name,
	                             'merge fraud 2 SB_BUF.PILOT_1006',
	                             i_cnt_changes                      => sql%rowcount,
	                             i_dt_start                         => v_start_dt,
	                             i_dt_finish                        => v_end_dt);                            

    -- фрод на возраст
    merge  --+ parallel (4)
    into
    SB_BUF.PILOT_1006  a
    using (Select f.*,
           rank() over (partition by  f.UCP_ID order by ODS$VALIDFROM desc) as r
           from sb_src.erib_users f) t
    on (a.CLIENT_EPK = t.UCP_ID
        and months_between(operation_date, BIRTHDAY) / 12 > 70
        --and a.operation_date between v_start_dt and v_end_dt
        AND trunc(a.operation_date) between v_start_dt and v_end_dt 
        AND a.SOURCE_SYSTEM_CODE in ('PIFIYA','ASBS','CBDBO')
        AND a.OPERATION_TYPE_ID IN ('ИСЖ','НСЖ','ПИФ','ЗПИФ','ИИС','ДУ','BROKER_COMMISSION')
        and t.STATUS = 'A'
        and t.r =1
        AND a.rowid NOT IN (SELECT rw FROM					--Для этих страховок целевой клиент старше 70 лет, убираем из фрода
										(
										SELECT
										rowid AS rw,
										aa.*,
										CASE 
											WHEN aa.product_name='Семейный актив' and  (aa.PRODUCT_ITEM_ID LIKE 'НМД0А%'
																						AND aa.PRODUCT_ITEM_ID LIKE 'НМР0А%'
																						AND aa.PRODUCT_ITEM_ID LIKE 'ПМНР0%'
																						AND aa.PRODUCT_ITEM_ID LIKE 'ПМНД0%'
																						AND aa.PRODUCT_ITEM_ID LIKE 'ВМВД1А%')
																			THEN '1'
											WHEN aa.product_name='Наследие' 	THEN '1'
											ELSE '0'
											END flg
										FROM  SB_buf.PILOT_1006  aa
										WHERE 1=1
										--aa.TAB_NUM IS NOT NULL
										AND trunc(aa.operation_date) between v_start_dt and v_end_dt
										)
									WHERE flg=1	
							)
      
        )
    when matched then
      update set a.is_fraud = 2, a.in_motiv = 0;

    sb_utl.pkg_log.p_add_log(c_pkg_name,
                             c_p_name,
                             'END merge fraud 2 into SB_BUF.PILOT_1006',
                             i_cnt_changes                          => sql%rowcount,
                             i_dt_start                             => v_start_dt,
                             i_dt_finish                            => v_end_dt);

    commit;
   
   
	sb_utl.pkg_log.p_add_log(c_pkg_name,
	                             c_p_name,
	                             'merge ПИФ fraud 3 SB_BUF.PILOT_1006',
	                             i_cnt_changes                      => sql%rowcount,
	                             i_dt_start                         => v_start_dt,
	                             i_dt_finish                        => v_end_dt);                            
   

--фрод на более 1 ПИФ в день

 merge  --+ parallel (4)
 into
    SB_BUF.PILOT_1006  a
    using (Select f.rowid AS rw,
           rank() over (partition by  f.CLIENT_EPK, f.OPERATION_DATE order by f.id_vdmrb desc) as r
           from SB_BUF.PILOT_1006 f
           WHERE
           	f.OPERATION_TYPE_ID IN ('ПИФ','ЗПИФ')) t
    on (a.rowid = t.rw
        and t.r>1
        )
    when matched then
      update set a.is_fraud = 3, a.in_motiv = 0;	 
     
         sb_utl.pkg_log.p_add_log(c_pkg_name,
                             c_p_name,
                             'END merge ПИФ fraud 3 into SB_BUF.PILOT_1006',
                             i_cnt_changes                          => sql%rowcount,
                             i_dt_start                             => v_start_dt,
                             i_dt_finish                            => v_end_dt);

    commit;


  sb_utl.pkg_log.p_add_log(c_pkg_name,
                             c_p_name,
                             'merge into SB_BUF.PILOT_1006 ИПП',
                             i_cnt_changes                      => sql%rowcount,
                             i_dt_start                         => v_start_dt,
                             i_dt_finish                        => v_end_dt);   

	merge --+ parallel (4)
	INTO 
     SB_BUF.PILOT_1006  a
    using (                            
				SELECT --+parallel(16) 
				DISTINCT
   					p.rowid rw, ltrim (t.EMPLOYEE_TAB ,'000') tab_num
				FROM SB_BUF.PILOT_1006 p
				JOIN asm_sandbox_blg.ipp_insurance_v2 t
				ON p.ID_VDMRB = t.num
				JOIN sb_fct.v_sap_dic_employee_org_pos ep  
					ON
					CAST(ltrim (t.EMPLOYEE_TAB,'000') AS varchar2(100))=CAST(ep.EMPLOYEE_ID AS varchar2(100)) 
					AND ep.POSITION_GROUP in ('ФМ','РО','ЗРО')  
					and trunc(CASE WHEN (p.OPERATION_DATE<>p.OPERATION_DATE_SRC AND p.OPERATION_DATE_SRC<p.REPORT_DATE) THEN p.OPERATION_DATE_SRC ELSE p.OPERATION_DATE end) between trunc(ep.START_DATE) and trunc(ep.END_DATE) 
				WHERE
					trunc(p.operation_date) between v_start_dt and v_end_dt --29/09/2021 апдейт после ввода даты отчета 
          and p.tab_num is null
            ) m
     ON (a.rowid = m.rw)
     WHEN MATCHED THEN 
    	UPDATE SET a.TAB_NUM = m.tab_num;
                            

     
    sb_utl.pkg_log.p_add_log(c_pkg_name,
                             c_p_name,
                             'END merge into SB_BUF.PILOT_1006 ИПП',
                             i_cnt_changes                          => sql%rowcount,
                             i_dt_start                             => v_start_dt,
                             i_dt_finish                            => v_end_dt);

    commit;   
   

  sb_utl.pkg_log.p_add_log(c_pkg_name,
                             c_p_name,
                             'merge employee_id sbolpro into SB_BUF.PILOT_1006 ',
                             i_cnt_changes                      => sql%rowcount,
                             i_dt_start                         => v_start_dt,
                             i_dt_finish                        => v_end_dt);   

	merge --+ parallel (16)
	INTO 
     SB_BUF.PILOT_1006  a
    using (                       
    						SELECT *
							FROM (
									SELECT --+parallel(16)
									DISTINCT
										row_number() over(PARTITION BY o.rowid ORDER BY c.DATA_TIMESTAMP DESC ,c.EMPLOYEEPERSONALNUMBER asc) num_of_row , --последний по дате, первый по ТН (коммент от 23/12/2021, сделано ранее)
										o.rowid as rw, 
										ltrim (c.EMPLOYEEPERSONALNUMBER,'000') AS tab_num--,
										--o.*
									from SB_BUF.PILOT_1006 o 
									join (
                     									SELECT --+parallel(16)
											DISTINCT
												
												 trunc(ess.DT_START) DATA_TIMESTAMP, 
												ess.EMPLOYEE_ID as EMPLOYEEPERSONALNUMBER, 
												ess.PPRB_ID AS EPK_ID 
											FROM sb_src.EFS_SBOLPRO_SESSIONS ess
											WHERE ess.EMPLOYEE_ID IS NOT null
										) c
									ON
									    o.CLIENT_EPK = c.EPK_ID 
									    AND (trunc(C.DATA_TIMESTAMP) BETWEEN TRUNC(CASE WHEN (O.OPERATION_DATE<>o.OPERATION_DATE_SRC AND o.OPERATION_DATE_SRC<o.REPORT_DATE) THEN o.OPERATION_DATE_SRC ELSE o.OPERATION_DATE end)-5 AND TRUNC(CASE WHEN (O.OPERATION_DATE<>o.OPERATION_DATE_SRC AND o.OPERATION_DATE_SRC<o.REPORT_DATE) THEN o.OPERATION_DATE_SRC ELSE o.OPERATION_DATE end))
									    AND ltrim (c.EMPLOYEEPERSONALNUMBER,'000') IS NOT NULL
									    AND o.TAB_NUM IS null
									JOIN sb_fct.v_sap_dic_employee_org_pos ep  
									ON
										ltrim (c.EMPLOYEEPERSONALNUMBER,'000')=CAST(ep.EMPLOYEE_ID AS varchar2(100)) 
										AND ep.POSITION_GROUP in ('ФМ','РО','ЗРО')  
										and trunc(CASE WHEN (O.OPERATION_DATE<>o.OPERATION_DATE_SRC AND o.OPERATION_DATE_SRC<o.REPORT_DATE) THEN o.OPERATION_DATE_SRC ELSE o.OPERATION_DATE end) between trunc(ep.START_DATE) and trunc(ep.END_DATE)
									WHERE
										trunc(o.operation_date) between v_start_dt and v_end_dt  

								)  
							WHERE num_of_row=1
						
            ) t
     ON (a.rowid = t.rw)
     WHEN MATCHED THEN 
    	UPDATE SET a.TAB_NUM = t.tab_num
    ;
                            

     
    sb_utl.pkg_log.p_add_log(c_pkg_name,
                             c_p_name,
                             'END merge employee_ID sbolpro into SB_BUF.PILOT_1006 sbolpro',
                             i_cnt_changes                          => sql%rowcount,
                             i_dt_start                             => v_start_dt,
                             i_dt_finish                            => v_end_dt);

    commit;      
   
   
   sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'merge employee_id CRM to SB_BUF.PILOT_1006',
                                    i_dt_start => v_start_dt, i_dt_finish => v_end_dt);  

          MERGE  --+parallel(4) 
          INTO
SB_BUF.PILOT_1006 F USING 
( select --+parallel(16)
* FROM (
				SELECT --+no_index (C IDX4_CRM_OPS) 
			    DISTINCT 
			    row_number() over(PARTITION BY o.rowid ORDER BY c.DTTM desc,c.EMPLOYEE_ID asc) num_of_row ,
				O.ROWID RW, C.EMPLOYEE_ID
				FROM SB_BUF.PILOT_1006 O 
				JOIN SB_SRC.CRM_OPS C 
				ON O.CLIENT_EPK = C.EPK_ID 
					AND (trunc(C.DTTM) BETWEEN TRUNC(CASE WHEN (O.OPERATION_DATE<>o.OPERATION_DATE_SRC AND o.OPERATION_DATE_SRC<o.REPORT_DATE) THEN o.OPERATION_DATE_SRC ELSE o.OPERATION_DATE end)-5 AND TRUNC(CASE WHEN (O.OPERATION_DATE<>o.OPERATION_DATE_SRC AND o.OPERATION_DATE_SRC<o.REPORT_DATE) THEN o.OPERATION_DATE_SRC ELSE o.OPERATION_DATE end)) 
					AND o.TAB_NUM IS null
				JOIN SB_FCT.V_SAP_DIC_EMPLOYEE_ORG_POS EP 
				ON CAST(C.EMPLOYEE_ID AS VARCHAR2(100))=CAST(EP.EMPLOYEE_ID AS VARCHAR2(100))
					AND EP.POSITION_GROUP in ('ФМ','РО','ЗРО') 
					AND (TRUNC(CASE WHEN (O.OPERATION_DATE<>o.OPERATION_DATE_SRC AND o.OPERATION_DATE_SRC<o.REPORT_DATE) THEN o.OPERATION_DATE_SRC ELSE o.OPERATION_DATE end) BETWEEN TRUNC(EP.START_DATE) AND TRUNC(EP.END_DATE)) 
					WHERE 1=1 
						AND trunc(o.operation_date) between v_start_dt and v_end_dt 
				)
	WHERE num_of_row=1
) B
   ON ( B.RW = F.ROWID ) 
  WHEN MATCHED THEN UPDATE SET F.TAB_NUM = B.EMPLOYEE_ID
 ;                                   

 	sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'end merge employee_id CRM to SB_BUF.PILOT_1006',
                i_cnt_changes => sql%rowcount, i_dt_start => v_start_dt, i_dt_finish => v_end_dt);
 
COMMIT; 
 
          MERGE  --+parallel(4) 
          INTO
SB_BUF.PILOT_1006 F USING 
( select --+parallel(16)
* FROM (
	SELECT --+no_index (C IDX4_CRM_OPS) 
    DISTINCT 
    row_number() over(PARTITION BY o.rowid ORDER BY c.DTTM_UPD desc,c.EMPLOYEE_ID asc) num_of_row ,
	O.ROWID RW, C.EMPLOYEE_ID
	FROM SB_BUF.PILOT_1006 O 
	JOIN SB_SRC.CRM_OPS C 
	ON O.CLIENT_EPK = C.EPK_ID 
		AND (trunc(C.DTTM_UPD) BETWEEN TRUNC(CASE WHEN (O.OPERATION_DATE<>o.OPERATION_DATE_SRC AND o.OPERATION_DATE_SRC<o.REPORT_DATE) THEN o.OPERATION_DATE_SRC ELSE o.OPERATION_DATE end)-5 AND TRUNC(CASE WHEN (O.OPERATION_DATE<>o.OPERATION_DATE_SRC AND o.OPERATION_DATE_SRC<o.REPORT_DATE) THEN o.OPERATION_DATE_SRC ELSE o.OPERATION_DATE end)) 
		AND o.TAB_NUM IS null
	JOIN SB_FCT.V_SAP_DIC_EMPLOYEE_ORG_POS EP 
	ON CAST(C.EMPLOYEE_ID AS VARCHAR2(100))=CAST(EP.EMPLOYEE_ID AS VARCHAR2(100))
		AND EP.POSITION_GROUP in ('ФМ','РО','ЗРО') 
		AND (TRUNC(CASE WHEN (O.OPERATION_DATE<>o.OPERATION_DATE_SRC AND o.OPERATION_DATE_SRC<o.REPORT_DATE) THEN o.OPERATION_DATE_SRC ELSE o.OPERATION_DATE end) BETWEEN TRUNC(EP.START_DATE) AND TRUNC(EP.END_DATE)) 
		WHERE 1=1
			AND trunc(o.operation_date) between v_start_dt and v_end_dt 
			
		)	
	WHERE num_of_row=1
) B
   ON ( B.RW = F.ROWID) 
  WHEN MATCHED THEN UPDATE SET F.TAB_NUM = B.EMPLOYEE_ID
 ;



	sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'end merge employee_id CRM to SB_BUF.PILOT_1006',
                i_cnt_changes => sql%rowcount, i_dt_start => v_start_dt, i_dt_finish => v_end_dt);
               
	COMMIT;     
   
   

   
   
--Тянем табельники для ИОС 

sb_utl.pkg_log.p_add_log(c_pkg_name,
                             c_p_name,
                             'merge employee_id для ИОС sbolpro_events into SB_BUF.PILOT_1006 ',
                             i_cnt_changes                      => sql%rowcount,
                             i_dt_start                         => v_start_dt,
                             i_dt_finish                        => v_end_dt);   

merge --+ parallel (16)
INTO 
 SB_BUF.PILOT_1006  a
using (      
   SELECT * FROM (	
				SELECT --+parallel(16)
				distinct
				row_number()over(PARTITION BY p.ID_VDMRB,p.OPERATION_DATE,p.OPERATION_NAME ORDER BY c.DATA_TIMESTAMP) AS rn,
				p.rowid AS rw,
				c.DATA_TIMESTAMP ,
				c.EMPLOYEEPERSONALNUMBER AS EMPLOYEE_ID,
				ep.POSITION_GROUP ,
				p.*
				FROM sb_buf.PILOT_1006 p 
				JOIN (SELECT --+parallel(16)
						DISTINCT
							 trunc(DATA_TIMESTAMP) DATA_TIMESTAMP, 
							EMPLOYEEPERSONALNUMBER, 
							EPK_ID 
						FROM sb_src.sbolpro_events se
						 where EMPLOYEEPERSONALNUMBER is not NULL
						 	AND se.CODE IN ('FM_IOS'))  c
				ON c.epk_id = p.CLIENT_EPK 
				AND c.DATA_TIMESTAMP BETWEEN TRUNC(p.OPERATION_DATE,'mm') AND TRUNC(p.OPERATION_DATE)
				
				JOIN sb_fct.v_sap_dic_employee_org_pos ep  
				ON
				CAST(ltrim (c.EMPLOYEEPERSONALNUMBER,'000') AS varchar2(100))=CAST(ep.EMPLOYEE_ID AS varchar2(100)) 
				AND ep.POSITION_GROUP in ('ФМ','РО','ЗРО')  
				and c.DATA_TIMESTAMP between trunc(ep.START_DATE) and trunc(ep.END_DATE) 
				WHERE p.PRODUCT_NAME IN ('Инвестиционные облигации Сбербанка на 3 года', 'Инвестиционные облигации Сбербанка на 5 лет')
				--AND p.TAB_NUM IS NULL
				AND trunc(p.operation_date) between v_start_dt and v_end_dt 
				ORDER BY p.ID_VDMRB , /*ess.DT_START ,*/ row_number()over(PARTITION BY p.ID_VDMRB,p.OPERATION_DATE,p.OPERATION_NAME ORDER BY c.DATA_TIMESTAMP)
			)
	WHERE rn=1
        ) t
 ON (a.rowid = t.rw)
 WHEN MATCHED THEN 
	UPDATE SET a.TAB_NUM = t.EMPLOYEE_ID
    ;
                            

     
    sb_utl.pkg_log.p_add_log(c_pkg_name,
                             c_p_name,
                             'END merge employee_ID для ИОС sbolpro_events into SB_BUF.PILOT_1006 sbolpro',
                             i_cnt_changes                          => sql%rowcount,
                             i_dt_start                             => v_start_dt,
                             i_dt_finish                            => v_end_dt);

    commit; 

   
   --Добиваем ТН по ИОС по сессиям сболпро, если в events не нашлись строки
  sb_utl.pkg_log.p_add_log(c_pkg_name,
                             c_p_name,
                             'merge employee_id для ИОС sbolpro into SB_BUF.PILOT_1006 ',
                             i_cnt_changes                      => sql%rowcount,
                             i_dt_start                         => v_start_dt,
                             i_dt_finish                        => v_end_dt);   

merge --+ parallel (16)
INTO 
 SB_BUF.PILOT_1006  a
using (      
   SELECT * FROM (	
		SELECT --+parallel(16)
		distinct
		row_number()over(PARTITION BY p.ID_VDMRB,p.OPERATION_DATE,p.OPERATION_NAME ORDER BY trunc(ess.DT_START) DESC, ess.EMPLOYEE_ID asc) AS rn, --последний по дате, первый по ТН 23/12/2021
		p.rowid AS rw,
		ess.DT_START ,
		ess.EMPLOYEE_ID,
		ep.POSITION_GROUP ,
		p.*
		FROM sb_buf.PILOT_1006 p 
		JOIN sb_src.EFS_SBOLPRO_SESSIONS ess  
		ON ess.PPRB_ID = p.CLIENT_EPK 
		AND trunc(ess.DT_START) BETWEEN TRUNC(p.OPERATION_DATE,'mm') AND TRUNC(p.OPERATION_DATE)
		JOIN sb_fct.v_sap_dic_employee_org_pos ep  
		ON
		CAST(ltrim (ess.EMPLOYEE_ID,'000') AS varchar2(100))=CAST(ep.EMPLOYEE_ID AS varchar2(100)) 
		AND ep.POSITION_GROUP in ('ФМ','РО','ЗРО')  
		and trunc(ess.DT_START) between trunc(ep.START_DATE) and trunc(ep.END_DATE) 
		WHERE p.PRODUCT_NAME IN ('Инвестиционные облигации Сбербанка на 3 года', 'Инвестиционные облигации Сбербанка на 5 лет')
		AND p.TAB_NUM IS NULL
		AND trunc(p.operation_date) between v_start_dt and v_end_dt 
			)
	WHERE rn=1
        ) t
 ON (a.rowid = t.rw)
 WHEN MATCHED THEN 
	UPDATE SET a.TAB_NUM = t.EMPLOYEE_ID
    ;
                            

     
    sb_utl.pkg_log.p_add_log(c_pkg_name,
                             c_p_name,
                             'END merge employee_ID для ИОС sbolpro into SB_BUF.PILOT_1006 sbolpro',
                             i_cnt_changes                          => sql%rowcount,
                             i_dt_start                             => v_start_dt,
                             i_dt_finish                            => v_end_dt);

    commit;    
   
   
      
   
--Доразмечаем табельники по пифам из пифии (не 100% совпадение, человеческий фактор. Но лучше, чем null)

  sb_utl.pkg_log.p_add_log(c_pkg_name,
                             c_p_name,
                             'merge into SB_BUF.PILOT_1006 ПИФИЯ табельники',
                             i_cnt_changes                      => sql%rowcount,
                             i_dt_start                         => v_start_dt,
                             i_dt_finish                        => v_end_dt); 
   
MERGE --+ parallel (4)
INTO sb_buf.PILOT_1006 p
USING (
		SELECT 	
		k.rowid AS rw,
		k.ID_VDMRB, 
		k.OPERATION_TYPE_ID , 
		k.PRODUCT_ITEM_ID, 
		k.OPERATION_DATE, 
		k.CLIENT_EPK, 
		k.IS_FRAUD, 
		k.IN_MOTIV, 
		k.KD, 
		k.TAB_NUM, 
		ltrim(h.PRSNNEL_NUM, '000') PRSNNEL_NUM, 
		CASE WHEN TAB_NUM=ltrim(h.PRSNNEL_NUM, '000') THEN 1 ELSE 0 END AS tab_num_flg
		FROM SB_buf.PILOT_1006 k
		LEFT JOIN
			(select * 
			from ASM_SANDBOX.FCT_PIFIYA_APPL t
			     left join sb_src.pifiya_appl_emp_lst q
			     on q.appl_id = t.appl_id
			        left join sb_src.pifiya_emp_idfn w
			        on w.emp_id = q.emp_id
			 ) h
		ON k.PRODUCT_ITEM_ID = h.appl_num
		JOIN sb_fct.v_sap_dic_employee_org_pos ep  
				ON
					ltrim (h.PRSNNEL_NUM,'000')=CAST(ep.EMPLOYEE_ID AS varchar2(100)) 
					AND ep.POSITION_GROUP in ('ФМ','РО','ЗРО')  
					and trunc(CASE WHEN (k.OPERATION_DATE<>k.OPERATION_DATE_SRC AND k.OPERATION_DATE_SRC<k.REPORT_DATE) THEN k.OPERATION_DATE_SRC ELSE k.OPERATION_DATE end) between trunc(ep.START_DATE) and trunc(ep.END_DATE)
		WHERE k.OPERATION_DATE BETWEEN v_start_dt AND v_end_dt
				AND k.SOURCE_SYSTEM_CODE = 'PIFIYA'
				AND (k.TAB_NUM IS NULL and h.PRSNNEL_NUM IS not null)
		) j
ON (j.rw=p.rowid)
WHEN MATCHED THEN UPDATE
SET p.TAB_NUM = j.PRSNNEL_NUM
;

    sb_utl.pkg_log.p_add_log(c_pkg_name,
                             c_p_name,
                             'END merge into SB_BUF.PILOT_1006 ПИФИЯ табельники',
                             i_cnt_changes                          => sql%rowcount,
                             i_dt_start                             => v_start_dt,
                             i_dt_finish                            => v_end_dt);
                            
COMMIT;                            
   



--Убираем фрод 3 (по кол-ву ПИФ в день) для территорий, рассчитываемых по КД

	sb_utl.pkg_log.p_add_log(c_pkg_name,
	                             c_p_name,
	                             'merge ПИФ del fraud 3 for KD SB_BUF.PILOT_1006',
	                             i_cnt_changes                      => sql%rowcount,
	                             i_dt_start                         => v_start_dt,
	                             i_dt_finish                        => v_end_dt);                            
   



 merge  --+ parallel (4)
 into
    SB_BUF.PILOT_1006  a
    using (Select fo.rowid AS rw,
    		ep.TERBANK,
    		fo.IS_FRAUD
           from SB_BUF.PILOT_1006 fo
           JOIN sb_fct.v_sap_dic_employee_org_pos ep  
					ON
					CAST(ltrim (fo.TAB_NUM ,'000') AS varchar2(100))=CAST(ep.EMPLOYEE_ID AS varchar2(100)) 
					AND ep.POSITION_GROUP in ('ФМ','РО','ЗРО')  
					and trunc(fo.OPERATION_DATE_src) between trunc(ep.START_DATE) and trunc(ep.END_DATE) 
           WHERE
           	fo.OPERATION_TYPE_ID IN ('ПИФ','ЗПИФ')
           	AND fo.IS_FRAUD = 3
           	AND ep.TERBANK IN 	('40',
								'44',
								'54',
								'55',
								'70')
			) t
    on (a.rowid = t.rw)
    when matched then
      update set a.is_fraud = 0;	 
     
         sb_utl.pkg_log.p_add_log(c_pkg_name,
                             c_p_name,
                             'END merge ПИФ del fraud 3 for KD into SB_BUF.PILOT_1006',
                             i_cnt_changes                          => sql%rowcount,
                             i_dt_start                             => v_start_dt,
                             i_dt_finish                            => v_end_dt);

    commit;






   
  sb_utl.pkg_log.p_add_log(c_pkg_name,
                             c_p_name,
                             'update in_motiv=3 SB_BUF.PILOT_1006',
                             i_cnt_changes                      => sql%rowcount,
                             i_dt_start                         => v_start_dt,
                             i_dt_finish                        => v_end_dt);   
                                   
                                   
                                   
--in_motive = 3
    update SB_BUF.PILOT_1006 x
       	set in_motiv = 3
     where x.TAB_NUM IS NOT null
     	--AND x.OPERATION_TYPE_ID ='ПИФ'
		AND x.IS_FRAUD =0
       	AND trunc(operation_date) between v_start_dt and v_end_dt --29/09/2021 апдейт после ввода даты отчета
		--and operation_date between v_start_dt and v_end_dt
       AND x.TAB_NUM IN (
      					SELECT DISTINCT o.tab_num FROM SB_BUF.PILOT_1006 o
      					JOIN sb_fct.v_sap_dic_employee_org_pos ep  
						ON
					o.TAB_NUM =CAST(ep.EMPLOYEE_ID AS varchar2(100)) 
					AND ep.POSITION_GROUP in ('ФМ','РО','ЗРО')  
					and trunc(CASE WHEN (o.OPERATION_DATE<>o.OPERATION_DATE_SRC AND o.OPERATION_DATE_SRC<o.REPORT_DATE) THEN o.OPERATION_DATE_SRC ELSE o.OPERATION_DATE end) between trunc(ep.START_DATE) and trunc(ep.END_DATE));

       
   sb_utl.pkg_log.p_add_log(c_pkg_name,
                             c_p_name,
                             'end update in_motiv=3 SB_BUF.PILOT_1006',
                             i_cnt_changes                          => sql%rowcount,
                             i_dt_start                             => v_start_dt,
                             i_dt_finish                            => v_end_dt);
       
               
	COMMIT;       

 
    sb_utl.pkg_log.p_add_log(c_pkg_name,
                             c_p_name,
                             'delete from sb_fct.PILOT_1006',
                             i_cnt_changes                               => sql%rowcount,
                             i_dt_start                                  => v_start_dt,
                             i_dt_finish                                 => v_end_dt);

DELETE FROM sb_fct.PILOT_1006 p 
WHERE 
trunc(p.operation_date) between v_start_dt and v_end_dt --29/09/2021 апдейт после ввода даты отчета
AND p.IN_MOTIV IN ('0','3')
;

    sb_utl.pkg_log.p_add_log(c_pkg_name,
                             c_p_name,
                             'END delete from sb_fct.PILOT_1006',
                             i_cnt_changes                                   => sql%rowcount,
                             i_dt_start                                      => v_start_dt,
                             i_dt_finish                                     => v_end_dt);

COMMIT;



    sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'insert SB_FCT.PILOT_1006 <- select sb_buf.PILOT_1006',
                                    i_dt_start => v_start_dt, i_dt_finish => v_end_dt);

INSERT INTO sb_fct.PILOT_1006 
    (
    SOURCE_SYSTEM_CODE, 
	ID_VDMRB, 
	PRODUCT_ID, 
	PRODUCT_NAME, 
	OPERATION_TYPE_ID, 
	OPERATION_NAME, 
	AMOUNT_RUR, 
	PRODUCT_ITEM_ID, 
	CHNL, 
	employee_fio,
	TAB_NUM, 
	CLIENT_FIO, 
	REPORT_DATE, 
	OPERATION_DATE, 
	OPERATION_DATE_SRC,
	CLIENT_EPK,
	is_fraud,
	in_motiv,
	in_motiv_kd,
	  cnt,
	  KD,
	  id_vdmrb_uniq,
	  date_upd,
	  AMOUNT_CURR,
	  CURRENCY
	) 
SELECT 	
    SOURCE_SYSTEM_CODE, 
	ID_VDMRB, 
	PRODUCT_ID, 
	PRODUCT_NAME, 
	OPERATION_TYPE_ID, 
	OPERATION_NAME, 
	AMOUNT_RUR, 
	PRODUCT_ITEM_ID, 
	CHNL, 
	employee_fio,
	TAB_NUM, 
	CLIENT_FIO, 
	REPORT_DATE, 
	OPERATION_DATE, 
	OPERATION_DATE_SRC,
	CLIENT_EPK,
	is_fraud,
	in_motiv,
	'1' AS in_motiv_kd,
	  cnt,
	  KD,
	  id_vdmrb_uniq AS id_vdmrb_uniq,
	  sysdate AS date_upd,
	  p2.AMOUNT_CURR,
	  p2.CURRENCY
FROM sb_buf.PILOT_1006 p2;

    sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'inserted into SB_FCT.PILOT_1006',
                i_cnt_changes => sql%rowcount, i_dt_start => v_start_dt, i_dt_finish => v_end_dt);

COMMIT;



--ставим in_motiv_kd 0 для пополнений одного счёта ИПП

    sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'KD=0 для Пополнений ИПП SB_fct.PILOT_1006',
                                    i_dt_start => v_start_dt, i_dt_finish => v_end_dt);

UPDATE SB_fct.PILOT_1006 ff
SET ff.IN_MOTIV_KD = 0
where ff.rowid in
(
	select rid from (
					select --+ parallel(15)
					op.rowid as rid,
					op.*,
					count(*)over(partition by op.TAB_NUM ,op.product_item_id,op.CLIENT_EPK,op.source_system_code,op.operation_date) as kkk
					 from SB_fct.PILOT_1006 op
					where 
					trunc(op.operation_date) between v_start_dt and v_end_dt
					--op.operation_date between v_start_dt and v_end_dt
					and op.product_item_id like '999-%'
					)
where kkk >1
and in_motiv = 3
);

    sb_utl.pkg_log.p_add_log(c_pkg_name, c_p_name, 'end KD=0 для Пополнений ИПП SB_fct.PILOT_1006',
                i_cnt_changes => sql%rowcount, i_dt_start => v_start_dt, i_dt_finish => v_end_dt);

COMMIT;




    sb_utl.pkg_log.p_add_log(c_pkg_name,
                             c_p_name,
                             'delete from ASM_SANDBOX.BUF_FCT_OPERATIONS',
                             i_cnt_changes                               => sql%rowcount,
                             i_dt_start                                  => v_start_dt,
                             i_dt_finish                                 => v_end_dt);


    delete from asm_sandbox.buf_fct_operations
     where 1=1
       AND source_system_code in ('PIFIYA','ASBS','CBDBO')
       AND OPERATION_TYPE_ID IN ('Оформление ИПП\КПП','ИСЖ','НСЖ','ПИФ','ЗПИФ','ИИС','ДУ','BROKER_COMMISSION','BROKER_ACCOUNT')
       and in_motiv in (3,0)
       AND trunc(operation_date) between v_start_dt and v_end_dt 
      ;

    sb_utl.pkg_log.p_add_log(c_pkg_name,
                             c_p_name,
                             'END delete from ASM_SANDBOX.BUF_FCT_OPERATIONS',
                             i_cnt_changes                                   => sql%rowcount,
                             i_dt_start                                      => v_start_dt,
                             i_dt_finish                                     => v_end_dt);

    commit;


    sb_utl.pkg_log.p_add_log(c_pkg_name,
                             c_p_name,
                             'insert into asm_sandbox.buf_fct_operations',
                             i_cnt_changes                               => sql%rowcount,
                             i_dt_start                                  => v_start_dt,
                             i_dt_finish                                 => v_end_dt);

    insert into asm_sandbox.buf_fct_operations
      (employee_id,
       employee_login,
       employee_fio,
       organization_unit_id,
       organization_tb_id,
       organization_gosb_id,
       organization_osb_id,
       organization_vsp_id,
       source_system_code,
       id_vdmrb,
       operation_date,
       product_id,
       product_name,
       operation_type_id,
       operation_name,
       currency,
       amount_curr,
       amount_rur,
       quantity,
       is_fraud,
       product_item_id,
       epk_id,
       deposit_term,
       credit_term,
       payment_frequency,
       certificate_nominal,
       total_metal_weight,
       insurance_amount,
       parent_id,
       first_payment,
       second_hand_confirmation,
       payment_status,
       pfr_confirmation,
       pfr_confirmation_date,
       agreement_num,
       --tab_num,
       product_id_rdm,
       operation_type_id_rdm,
       urf_code_actual,
       id_vdmrb_unique,
       organization_unit_id_rdm,
       --employee_id_rdm,
       chnl,
       in_motiv,
       last_update_date,
       operation_date_src,
       client_fio)
      select a.tab_num as employee_id,
             null employee_login,
             a.employee_fio as employee_fio,
             null organization_unit_id,
             null organization_tb_id,
             null organization_gosb_id,
             null organization_osb_id,
             null organization_vsp_id,
             a.source_system_code as source_system_code,
             a.ID_VDMRB as id_vdmrb,
             trunc(a.operation_date), 
             a.product_id AS product_id,
             a.PRODUCT_NAME AS product_name,
             a.operation_type_id as operation_type_id,
             a.OPERATION_NAME AS operation_name,
             a.CURRENCY AS currency,
             a.AMOUNT_CURR AS amount_curr,
             a.AMOUNT_RUR as amount_rur,
             '1' quantity,
             a.is_fraud is_fraud,
             a.PRODUCT_ITEM_ID as product_item_id,
             a.client_epk epk_id,
             null deposit_term,
             null credit_term,
             null payment_frequency,
             a.KD  AS certificate_nominal,
             null total_metal_weight,
             null insurance_amount,
             null parent_id,
             null first_payment,
             null second_hand_confirmation,
             null payment_status,
             null pfr_confirmation,
             null pfr_confirmation_date,
             a.PRODUCT_ITEM_ID agreement_num,
             null product_id_rdm,
             null operation_type_id_rdm,
             null urf_code_actual,
             null id_vdmrb_unique,
             null organization_unit_id_rdm,
             --employee_id_rdm,
             a.CHNL AS chnl,
             
            a.IN_MOTIV, 
           
             
             trunc(sysdate),
             a.operation_date_src as operation_date_src,
             a.CLIENT_FIO AS client_fio
        from sb_buf.PILOT_1006 a
       where a.OPERATION_DATE >= v_start_dt 
         and a.OPERATION_DATE <= v_end_dt
        AND a.TAB_NUM IS NOT null;

    sb_utl.pkg_log.p_add_log(c_pkg_name,
                             c_p_name,
                             'END insert into asm_sandbox.buf_fct_operations',
                             i_cnt_changes                                   => sql%rowcount,
                             i_dt_start                                      => v_start_dt,
                             i_dt_finish                                     => v_end_dt);

    commit;


    ---- ТУТ МАПИНГ


    sb_utl.pkg_log.p_add_log(c_pkg_name,
                             c_p_name,
                             'MAPPING',
                             i_dt_start  => v_start_dt,
                             i_dt_finish => v_end_dt);

    asm_sandbox.sp_operations_mapping('PIFIYA');
    asm_sandbox.sp_operations_mapping('ASBS');
    asm_sandbox.sp_operations_mapping('CBDBO');

    sb_utl.pkg_log.p_add_log(c_pkg_name,
                             c_p_name,
                             'END MAPPING',
                             i_dt_start   => v_start_dt,
                             i_dt_finish  => v_end_dt);

               
   sb_utl.pkg_log.p_add_log(c_pkg_name,
                             c_p_name,
                             'delete from ASM_SANDBOX.FCT_OPERATIONS',
                             i_dt_start                              => v_start_dt,
                             i_dt_finish                             => v_end_dt);

    delete from asm_sandbox.fct_operations
     where source_system_code in ('PIFIYA', 'ASBS','CBDBO')
       and OPERATION_TYPE_ID IN ('Оформление ИПП\КПП','ИСЖ','НСЖ','ПИФ','ЗПИФ','ИИС','ДУ','BROKER_COMMISSION','BROKER_ACCOUNT')
       and in_motiv  in ('3','0','-1')
       AND trunc(operation_date) between v_start_dt and v_end_dt 
;

    sb_utl.pkg_log.p_add_log(c_pkg_name,
                             c_p_name,
                             'END delete from ASM_SANDBOX.FCT_OPERATIONS',
                             i_dt_start                                  => v_start_dt,
                             i_dt_finish                                 => v_end_dt);

    commit;

    sb_utl.pkg_log.p_add_log(c_pkg_name,
                             c_p_name,
                             'insert into asm_sandbox.fct_operations',
                             i_cnt_changes                           => sql%rowcount,
                             i_dt_start                              => v_start_dt,
                             i_dt_finish                             => v_end_dt);

    insert into asm_sandbox.fct_operations
      (employee_id,
       employee_login,
       employee_fio,
       organization_unit_id,
       organization_tb_id,
       organization_gosb_id,
       organization_osb_id,
       organization_vsp_id,
       source_system_code,
       id_vdmrb,
       operation_date,
       product_id,
       product_name,
       operation_type_id,
       operation_name,
       currency,
       amount_curr,
       amount_rur,
       quantity,
       is_fraud,
       product_item_id,
       epk_id,
       deposit_term,
       credit_term,
       payment_frequency,
       certificate_nominal,
       total_metal_weight,
       insurance_amount,
       parent_id,
       first_payment,
       second_hand_confirmation,
       payment_status,
       pfr_confirmation,
       pfr_confirmation_date,
       agreement_num,
       product_id_rdm,
       operation_type_id_rdm,
       urf_code_actual,
       id_vdmrb_unique,
       organization_unit_id_rdm,
       last_update_date,
       flag_update,
       chnl,
       in_motiv,
       acc,
       term,
       urf_code_uni,
       client_fio,
       operation_date_src,
       is_delayed,

       urf_code_sap,
       orgunit_sap)
      select
      employee_id,
       employee_login,
       employee_fio,
       organization_unit_id,
       organization_tb_id,
       organization_gosb_id,
       organization_osb_id,
       organization_vsp_id,
       source_system_code,
       id_vdmrb,
       operation_date,
       product_id,
       product_name,
       operation_type_id,
       operation_name,
       currency,
       amount_curr,
       amount_rur,
       quantity,
       is_fraud,
       product_item_id,
       epk_id,
       deposit_term,
       credit_term,
       payment_frequency,
       certificate_nominal,
       total_metal_weight,
       insurance_amount,
       parent_id,
       first_payment,
       second_hand_confirmation,
       payment_status,
       pfr_confirmation,
       pfr_confirmation_date,
       agreement_num,
       product_id_rdm,
       operation_type_id_rdm,
       urf_code_actual,
       id_vdmrb_unique,
       organization_unit_id_rdm,
       last_update_date,
       flag_update,
       chnl,
       in_motiv,
       acc,
       term,
       urf_code_uni,
       client_fio,
       operation_date_src,
       is_delayed,

       urf_code_sap,
       orgunit_sap
        from asm_sandbox.buf_fct_operations a
       where source_system_code in ('PIFIYA','ASBS','CBDBO')
       	 AND OPERATION_TYPE_ID IN ('Оформление ИПП\КПП','ИСЖ','НСЖ','ПИФ','ЗПИФ','ИИС','ДУ','BROKER_COMMISSION','BROKER_ACCOUNT')
         AND trunc(operation_date) between v_start_dt and v_end_dt 
                  ;

    sb_utl.pkg_log.p_add_log(c_pkg_name,
                             c_p_name,
                             'END insert into asm_sandbox.fct_operations',
                             i_cnt_changes                               => sql%rowcount,
                             i_dt_start                                  => v_start_dt,
                             i_dt_finish                                 => v_end_dt);
    commit;  
    
    
    --ФРОД учет продажи одного продукта несколькими сотрудниками. Продажи ФМ исключаем. (ИСЖ, НСЖ, ИПП)
   sb_utl.pkg_log.p_add_log(c_pkg_name,
                             c_p_name,
                             'merge into asm_sandbox.fct_operations',
                             i_cnt_changes                      => sql%rowcount,
                             i_dt_start                         => v_start_dt,
                             i_dt_finish                        => v_end_dt);   

	merge --+ parallel (4)
	INTO 
     asm_sandbox.fct_operations  a
    using (                            
				SELECT rw FROM 
								(
								SELECT --+parallel(16)
								--DISTINCT
								o.rowid rw,
								o.ID_VDMRB, 
								o.PRODUCT_ITEM_ID,
								o.OPERATION_DATE,
								o.EMPLOYEE_ID tab_num, 
								ep.POSITION_GROUP, 
								o.IS_FRAUD, 
								o.IN_MOTIV, 
								o.OPERATION_TYPE_ID, 
								count(*) over (partition by  o.PRODUCT_ITEM_ID,o.ID_VDMRB ,o.OPERATION_DATE) as r
								from asm_sandbox.fct_operations o 
								JOIN sb_fct.v_sap_dic_employee_org_pos ep  
								   ON
								   o.EMPLOYEE_ID=ep.EMPLOYEE_ID 
								   and trunc(o.OPERATION_DATE_src) between trunc(ep.START_DATE) and trunc(ep.END_DATE)
								WHERE
									trunc(o.operation_date) between v_start_dt and v_end_dt
								   	and trunc(o.operation_date) >= v_start_date 
								  	AND (o.PRODUCT_ID_RDM IN (	SELECT DISTINCT rdm_product_id
																FROM asm_sandbox.pr_asm_dic_product_calc
																WHERE 1=1
																AND CALC_PRODUCT_GROUP IN ('БАНКОВСКОЕ СТРАХОВАНИЕ')
																AND post_group in ('ФМ')
															)
										AND
										o.OPERATION_TYPE_ID_RDM IN (	SELECT DISTINCT rdm_operation_id
																		FROM asm_sandbox.pr_asm_dic_product_calc
																		WHERE 1=1
																		AND CALC_PRODUCT_GROUP IN ('БАНКОВСКОЕ СТРАХОВАНИЕ')
																		AND post_group in ('ФМ')
																	)
										)
								  	) ttt
							WHERE r>1 AND POSITION_GROUP in ('ФМ')
							
           ) t
     ON (a.rowid = t.rw)
     WHEN MATCHED THEN 
    	UPDATE SET a.in_motiv = -1, a.is_fraud = 9
    ;
                            

     
    sb_utl.pkg_log.p_add_log(c_pkg_name,
                             c_p_name,
                             'END merge into asm_sandbox.fct_operations',
                             i_cnt_changes                          => sql%rowcount,
                             i_dt_start                             => v_start_dt,
                             i_dt_finish                            => v_end_dt);

    commit;      
   


--удаление дублей  по ИПП (пересечение с тиражом)   
   sb_utl.pkg_log.p_add_log(c_pkg_name,
                             c_p_name,
                             'удаление дублей по ИПП (пересечение с тиражом)',
                             i_cnt_changes                      => sql%rowcount,
                             i_dt_start                         => v_start_dt,
                             i_dt_finish                        => v_end_dt);   
        
delete from asm_sandbox.fct_operations ff
where ff.rowid in
(
select rid from (
select --+ parallel(15)
                            op.rowid as rid,op.*,
count(*)over(partition by op.employee_id,op.product_item_id,op.epk_id,op.source_system_code,op.operation_date) cnt
 from asm_sandbox.fct_operations op
 join sb_fct.v_sap_dic_employee_org_pos ep 
 on op.operation_date between trunc(ep.START_DATE) and trunc(ep.END_DATE)
 AND ep.POSITION_GROUP in ('ФМ','РО','ЗРО')
 and op.employee_id = ep.EMPLOYEE_ID
 
where 1=1
AND trunc(op.operation_date) between v_start_dt and v_end_dt
and op.product_item_id like '999-%'
)
where cnt >1
and in_motiv = 3
);


sb_utl.pkg_log.p_add_log(c_pkg_name,
                             c_p_name,
                             'удаление дублей по ИПП (пересечение с тиражом)',
                             i_cnt_changes                           => sql%rowcount,
                             i_dt_start                              => v_start_dt,
                             i_dt_finish                             => v_end_dt);

commit;


	sb_utl.pkg_log.p_end_proc(c_pkg_name, c_p_name);

   END p_pilot_1006;
 
END PKG_PILOT_1006;
