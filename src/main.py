import datetime
import calendar
import sys

from config import settings
from logger import logger

import maria
#import iris

def from_month(year, month):
     year, month = (year-1, 12) if month - 1 < 1 else (year, month-1) 
     day = calendar.monthrange(year, month)[1]
     return {"start": datetime.date(year, month, 1), "end": datetime.date(year, month, day)}

def to_month(year, month):
     day = calendar.monthrange(year, month)[1]
     return  {"start": datetime.date(year, month, 1), "end": datetime.date(year, month, day)}

def get_dates():
     if len(sys.argv) == 2: #해당월
          year, month = int(sys.argv[1][:4]), int(sys.argv[1][4:])
          before_month = from_month(year, month)
          this_month=to_month(year,month)
     else:
          today = datetime.date.today() 
          temp = from_month(today.year, today.month)["start"]
          
          before_month = from_month(temp.year, temp.month)
          this_month=to_month(temp.year,temp.month)         

     return {
          "before_month_start" : before_month["start"].strftime('%Y-%m-%d'),
          "before_month_end" : before_month["end"].strftime('%Y-%m-%d'),
          "this_month_start" : this_month["start"].strftime('%Y-%m-%d'),
          "this_month_end" : this_month["end"].strftime('%Y-%m-%d'),
     }

infos = [
       {
        "table": "VISITOR_CATE_POI",
        "query": """ 
                    select
                         '{this_month_start}' as REG_DATE,
                         a.CTGRY_ID1 as CTGRY_ID, 
                         b.CTGRY_NM,
                         a.POI_ID,	
                         c.POI_NM, 
                         CONCAT(c.ADDR1, ' ', c.ADDR2) as ADDR,
                         IF(a.PAY_YN = 'Y', '유료', '무료') as PAY_YN, 
                    a.VIEW_CNT, 
                    a.LTTD, 
                    a.LGNT
                    FROM poi AS a
                    LEFT JOIN ctgry AS b ON a.CTGRY_ID1 = b.CTGRY_ID and b.LANG_ID = 1 and b.USE_YN = 'Y'
                    LEFT JOIN poi_dtl AS c ON a.POI_ID = c.POI_ID and c.LANG_ID = 1
                    WHERE a.PAY_YN in ('Y','N') and a.USE_YN = 'Y' and a.DISP_YN = 'Y' and a.CLSBIZ_YN = 'N'
        """
    }
    ,
    {
        "table": "VISITOR_EVENT",
        "query": """
                 select
                    {this_month_start} as REG_DATE,
                    a.EVT_ID,
                    a.TTLE, 
                    DATE_FORMAT(a.BGNG_DT, '%Y-%m-%d') as BGNG_DATE, 
                    DATE_FORMAT(a.END_DT, '%Y-%m-%d') as END_DATE,  
                    IF(a.FEE = '' , '-', a.FEE) as PAY, 
                    IF(a.EXTRL_URL = '', '-', a.EXTRL_URL) as EVT_URL, 
                    CONCAT('http://27.101.101.67', b.FILE_PATH, b.FILE_ORGL_NM) as IMG_PATH,
                    b.FILE_NO as IMG_NO_IN_SUWON_DB
               FROM evt AS a
               LEFT JOIN evt_img AS b ON a.EVT_ID = b.EVT_ID and b.LANG_ID = 1 and b.MAIN_IMG_YN = 'Y' and b.DISP_YN ='Y' and b.USE_YN = 'Y'
               WHERE a.LANG_ID = 1 and a.USE_YN = 'Y' and (a.END_DT >= '{this_month_start}' and a.END_DT <= '{this_month_end}')  
        """
    },
    {
        "table": "VISITOR_REVIEW_POI",
        "query": """
       	     select 
	'{this_month_start}' as REG_DATE,	
	b.POI_ID,
	c.POI_NM,
	b.SCORE,
	b.CNT
from (
	select 
		a.POI_ID, 
		a.total/a.cnt as SCORE, 
		CNT
	from (
		select 
			POI_ID, 
			count(POI_ID) as cnt, 
			sum(STPT) as total 
		from user_review where LANG_ID = 1 and REVIEW_TY = 0 and USE_YN = 'Y' and POI_ID != 'undefined' group by POI_ID
	) a  
)b 
LEFT JOIN poi_dtl AS c ON b.POI_ID = c.POI_ID and c.LANG_ID = 1
order by cnt desc limit 10
        """
    },
    {
        "table": "VISITOR_ADMIN_KWRD",
        "query": """
        			select 
                         * 
                         from (				
                              select '{this_month_start}' as reg_date, trim(SRCHW) as keywrd, count(SRCHW) as cnt from user_search where LANG_ID = 1 and SEARCH_DT >= '{this_month_start}' and SEARCH_DT <= '{this_month_end}' 
                              group by SRCHW order by cnt desc limit 5) a
                    union all
                    select 
                    * 
                         from (
                              select '{before_month_start}' as reg_date, trim(SRCHW) as keywrd, count(SRCHW) as cnt from user_search where LANG_ID = 1 and SEARCH_DT >= '{before_month_start}' and SEARCH_DT <= '{before_month_end}' 
                              group by SRCHW order by cnt desc limit 5
                    ) b

        """
    },
    {
        "table": "VISITOR_ADMIN_RESRV",
        "query": """
                select 
                    c.REG_DATE,
                    c.POI_ID,
                    d.POI_NM,
                    c.cnt
                    from (			
                         select 
                              * 
                              from (
                                   select '2022-11-01' as REG_DATE, POI_ID, count(POI_ID) as cnt from ord_bot 
                                   WHERE (ORD_STAT = 'AP' or ORD_STAT = 'AV') and ORD_DT >= '2022-11-01' and ORD_DT <= '2022-11-30' group by POI_ID order by cnt desc limit 5
                              ) a 
                         union all
                              select 
                              * 
                              from (
                                   select '2022-10-01' as REG_DATE, POI_ID, count(POI_ID) as cnt from ord_bot 
                                   WHERE (ORD_STAT = 'AP' or ORD_STAT = 'AV') and ORD_DT >= '2022-10-01' and ORD_DT <= '2022-10-30' group by POI_ID order by cnt desc limit 5
                              ) b
               ) c	inner join poi_dtl AS d on c.POI_ID = d.POI_ID and d.LANG_ID = 1
        """
    },
    {
        "table": "ADMIN_CATE_POI",
        "query": """
                    select  
                         '{this_month_start}' as REG_DATE,
                         f.CTGRY_ID,
                         f.CTGRY_NM, 
                         a.POI_ID,	
                         e.POI_NM, 
                         CONCAT(e.ADDR1, ' ', e.ADDR2) as ADDR,
                    IF(a.PAY_YN = 'Y', '유료', '무료') as FEE, 
                    a.LTTD, 
                    a.LGNT, 
                    a.VIEW_CNT,
                    IF(isnull(c.REVIEW_CNT), 0 , c.REVIEW_CNT) as REVIEW_CNT, 
                    IF(isnull(c.REVIEW_STPT), 0 , c.REVIEW_STPT) as REVIEW_STPT, 
                    IF(isnull(d.BOOKMARK_CNT), 0 , d.BOOKMARK_CNT) as BOOKMARK_CNT
                    FROM poi AS a
                    LEFT JOIN (
                         select 
                              POI_ID, 
                              count(REVIEW_ID) AS REVIEW_CNT,
                              round(sum(STPT) / count(REVIEW_ID), 1) AS REVIEW_STPT
                         FROM user_review
                         WHERE LANG_ID = 1 and USE_YN = 'Y' and DISP_YN = 'Y' and REVIEW_TY = 0
                         GROUP BY POI_ID) AS c 
                    ON a.POI_ID = c.POI_ID
                    LEFT JOIN (
                         select 
                              POI_ID, 
                              count(BOOKMARK_ID) AS BOOKMARK_CNT 
                         FROM user_bookmark 
                         WHERE LANG_ID = 1 and USE_YN = 'Y' 
                         GROUP BY POI_ID) AS d 
                    ON a.POI_ID = d.POI_ID 
                    INNER JOIN poi_dtl AS e ON a.POI_ID = e.POI_ID and e.LANG_ID = 1
                    INNER JOIN ctgry AS f ON a.CTGRY_ID1 = f.CTGRY_ID and f.LANG_ID = 1 and f.USE_YN = 'Y'
                    WHERE a.PAY_YN in ('Y','N') and a.USE_YN = 'Y' and a.DISP_YN = 'Y' and a.CLSBIZ_YN = 'N'
                    ORDER BY c.REVIEW_CNT DESC
        """
    },
    {
        "table": "ADMIN_USER_JOIN",
        "query": """
           select
               *
               from( 
                    select
                         '{this_month_start}' as REG_DATE,
                         IF(FRGNR_YN='N','내국인','외국인') as FRGNR,
                         IF(GENDER='M','남자','여자') as GNDER,
                         case 
                              when BRDT_DATE = 0 then '10대 미만'
                              when BRDT_DATE = 1 then '10대'
                              when BRDT_DATE = 2 then '20대'
                              when BRDT_DATE = 3 then '30대'
                              when BRDT_DATE = 4 then '40대'
                              when BRDT_DATE = 5 then '50대'
                              when BRDT_DATE >= 6 then '60대 이상'
                         end as AGE,
                         count(USER_ID) as JOIN_CNT
                    from (
                         select 
                              JOIN_DT,
                              USER_ID,
                              FRGNR_YN,
                              GENDER,
                              FLOOR((2022-DATE_FORMAT(BRDT,'%Y')+1)/10) as BRDT_DATE
                         from user
                         WHERE USE_YN = 'Y' and LANG_ID = 1 and FRGNR_YN in ('Y','N') and GENDER in ('M','F') and JOIN_DT >= '2022-10-01' and JOIN_DT <= '2022-10-30'
                    ) a group by a.FRGNR_YN, a.GENDER, a.BRDT_DATE
               ) b where b.age is not null
        """
    },
      {
        "table": "ADMIN_SALES",
        "query": """
               select 
                    '{this_month_start}' as REG_DATE,
                    d.SALES_TYPE,
                    d.TOTAL_AMT
                    from(
                         select date_format(REG_DT, '%Y-%m') as SALES_DATE, '트레볼루션' as SALES_TYPE, convert(sum(PAID_AMT), int) as TOTAL_AMT from ORD_BOT where ORD_STAT in ('AP','AV','EP') and REG_DT >= '{this_month_start}' and REG_DT <= '{this_month_end}' group by SALES_DATE 
                         union 
                         select date_format(REG_DT, '%Y-%m') as SALES_DATE, '숙박' as SALES_TYPE, convert(sum(PAID_AMT), int) as TOTAL_AMT from ORD_ROOM where ORD_STAT in ('P') and REG_DT >= '{this_month_start}' and REG_DT <= '{this_month_end}' group by SALES_DATE 
                         union
                         select date_format(REG_DT, '%Y-%m') as SALES_DATE,  '하이퍼클라우드' as SALES_TYPE, convert(sum(PAID_AMT), int) as TOTAL_AMT from ORD_SMT where ORD_STAT in ('P','A','E') and REG_DT >= '{this_month_start}' and REG_DT <= '{this_month_end}' group by SALES_DATE 
                         union
                         select 
                              c.SALES_DATE,
                              '무브' as SALES_TYPE,
                              convert(c.SC - c.RE, int) as TOTAL_AMT
                              from ( 
                                   select 
                                        date_format(REG_DT, '%Y-%m') as SALES_DATE,
                                        SUM( IF( ORD_STAT = 'RETURN', PAID_AMT, 0 ) )  as RE, 
                                        SUM( IF( ORD_STAT = 'PAY_SUCCESS', PAID_AMT, 0 ) )  as SC
                                   from ORD_MOVV where REG_DT >= '{this_month_start}' and REG_DT <= '{this_month_end}' group by SALES_DATE) as c
                    ) d	
        """
    }
    ]

def transform(table, cols, rows):
     if table == "VISITOR_ADMIN_KWRD" or table == "VISITOR_ADMIN_RESRV" :
          cols += ["RANK", "RANKNUM"]
          
          this = [row[1] for row in rows[:5]]
          before = [row[1] for row in rows[:5]]
          
          data = []
          for i in range(len(this)):
               temp = list(rows[i])
               temp.append(i+1)
               try : 
                    bfr_rank = before.index(this[i])
                    temp.append(bfr_rank - i)
               except ValueError:
                    temp.append(-100)
               finally:
                    data.append(tuple(temp))
     elif table == "ADMIN_SALES":
          check = ["트레볼루션", "하이퍼클라우드", "무브", "숙박"]
          date = ''
          data = []
          for row in rows:
               date = row[0]
               if row[1] in check:
                   data.append(row)
                   check.remove(row[1])
          for c in check:
               temp = tuple([date, c, 0])
               data.append(temp) 
     return {"cols" : cols, "data" : tuple(data)}

def main():
    try:
        logger.info("--")
        dates = get_dates()
        
        conditions = {
            "reg_date": dates["base_time"]
        }
        
        trans_table=["VISITOR_ADMIN_KWRD", "VISITOR_ADMIN_RESRV", "ADMIN_SALES"]
        for i in infos:
            if i['table'] == "VISITOR_REVIEW_POI":
               print("!!")
               print(i["query"].format(**dates))
               cols, rows = maria.fetch(i["query"].format(**dates))
               
               if i["table"] in trans_table:
                    data = transform(i["table"], cols, rows)
                    cols = data["cols"]
                    rows = data["data"]  
               print(cols, rows)
            iris.update(i["table"], cols, rows, conditions)
        logger.info("^^")
    except:
        logger.exception("oo")

if __name__ == "__main__":
     main()


