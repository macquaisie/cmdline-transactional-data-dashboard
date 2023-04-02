import cx_Oracle
import time
import os

while True:
    con1 = cx_Oracle.connect("db_user", "db_password", "10.0.0.1/testamerica")
    con2 = cx_Oracle.connect("db_user", "db_password", "10.0.0.2/testafrica")
    con3 = cx_Oracle.connect("db_user", "db_password", "10.0.0.3/testuk")
    con4 = cx_Oracle.connect("db_user", "db_password", "10.0.0.4/testasia")



    america = con1.cursor()
    africa = con2.cursor()
    uk = con3.cursor()
    asia = con4.cursor()

    query1 = america.execute ("select 'FAILED' STATUS, COUNT(*),SUM(CASE WHEN msg_status='F' THEN 1 ELSE 0 END ) AS FAILED,SUM(CASE WHEN msg_status='R' THEN 1 ELSE 0 END ) AS REPAIR,SUM(CASE WHEN msg_status='N' THEN 1 ELSE 0 END ) AS NOT_GEN, SUM(CASE WHEN msg_status='H' THEN 1 ELSE 0 END ) AS HANDED,SUM(CASE WHEN msg_status='W' THEN 1 ELSE 0 END ) AS PROCESSING,SUM(CASE WHEN msg_status='G' THEN 1 ELSE 0 END ) AS GENERATING frtestamerica.mstb_dly_msg_out where msg_type='CUST_TRANSFER' and msg_status in ('F', 'N', 'R', 'H', 'W', 'G') and TRUNC(branch_date)= TRUNC(SYSDATE)")

    query2 = africa.execute ("select 'FAILED' STATUS, COUNT(*),SUM(CASE WHEN msg_status='F' THEN 1 ELSE 0 END ) AS FAILED,SUM(CASE WHEN msg_status='R' THEN 1 ELSE 0 END ) AS REPAIR,SUM(CASE WHEN msg_status='N' THEN 1 ELSE 0 END ) AS NOT_GEN, SUM(CASE WHEN msg_status='H' THEN 1 ELSE 0 END ) AS HANDED,SUM(CASE WHEN msg_status='W' THEN 1 ELSE 0 END ) AS PROCESSING,SUM(CASE WHEN msg_status='G' THEN 1 ELSE 0 END ) AS GENERATING frtestafrica.mstb_dly_msg_out where msg_type='CUST_TRANSFER' and msg_status in ('F', 'N', 'R', 'H', 'W', 'G') and TRUNC(branch_date)= TRUNC(SYSDATE)")

    query3 = uk.execute ("select 'FAILED' STATUS, COUNT(*),SUM(CASE WHEN msg_status='F' THEN 1 ELSE 0 END ) AS FAILED,SUM(CASE WHEN msg_status='R' THEN 1 ELSE 0 END ) AS REPAIR,SUM(CASE WHEN msg_status='N' THEN 1 ELSE 0 END ) AS NOT_GEN, SUM(CASE WHEN msg_status='H' THEN 1 ELSE 0 END ) AS HANDED,SUM(CASE WHEN msg_status='W' THEN 1 ELSE 0 END ) AS PROCESSING,SUM(CASE WHEN msg_status='G' THEN 1 ELSE 0 END ) AS GENERATING frtestuk.mstb_dly_msg_out where msg_type='CUST_TRANSFER' and msg_status in ('F', 'N', 'R', 'H', 'W', 'G') and TRUNC(branch_date)= TRUNC(SYSDATE)")

    query4 = asia.execute ("select 'FAILED' STATUS, COUNT(*),SUM(CASE WHEN msg_status='F' THEN 1 ELSE 0 END ) AS FAILED,SUM(CASE WHEN msg_status='R' THEN 1 ELSE 0 END ) AS REPAIR,SUM(CASE WHEN msg_status='N' THEN 1 ELSE 0 END )  AS NOT_GEN, SUM(CASE WHEN msg_status='H' THEN 1 ELSE 0 END ) AS HANDED,SUM(CASE WHEN msg_status='W' THEN 1 ELSE 0 END ) AS PROCESSING, SUM(CASE WHEN msg_status='G' THEN 1 ELSE 0 END ) AS GENERATING frtestasia.mstb_dly_msg_out where msg_type='CUST_TRANSFER' and msg_status in ('F', 'N', 'R', 'H', 'W', 'G') and TRUNC(branch_date)= TRUNC(SYSDATE)")




    os.system('clear')
    for result in america:
        print ('    TOTAL NO. America TRANSCATIONS FOR TODAY =', result[1])
        print ('TOTAL NO. OF FAILED TRANSACTIONS     (F) =', result[2])
        print ('TOTAL NO. OF REPAIR TRANSACTIONS     (R) =', result[3])
        print ('TOTAL NO. OF NOT GEN TRANSACTIONS    (N) =', result[4])
        print ('TOTAL NO. OF HANDED OFF TRANSACTIONS (H) =', result[5])
        print ('TOTAL NO. OF PROCESSING TRANSACTIONS (W) =', result[6])
       	print ('TOTAL NO. OF GENERATING TRANSACTIONS (G) =', result[7])
        print ('____________________________________________')
        print (' ')
        print (' ')


        for result in africa:
            print ('    TOTAL NO. Africa TRANSCATIONS FOR TODAY =', result[1])
            print ('TOTAL NO. OF FAILED TRANSACTIONS     (F) =', result[2])
            print ('TOTAL NO. OF REPAIR TRANSACTIONS     (R) =', result[3])
            print ('TOTAL NO. OF NOT GEN TRANSACTIONS    (N) =', result[4])
            print ('TOTAL NO. OF HANDED OFF TRANSACTIONS (H) =', result[5])
            print ('TOTAL NO. OF PROCESSING TRANSACTIONS (W) =', result[6])
            print ('TOTAL NO. OF GENERATING TRANSACTIONS (G) =', result[7])
            print ('____________________________________________')
            print (' ')
            print (' ')


            for result in uk:
                print ('    TOTAL NO. UK TRANSCATIONS FOR TODAY =', result[1])
                print ('TOTAL NO. OF FAILED TRANSACTIONS     (F) =', result[2])
                print ('TOTAL NO. OF REPAIR TRANSACTIONS     (R) =', result[3])
                print ('TOTAL NO. OF NOT GEN TRANSACTIONS    (N) =', result[4])
                print ('TOTAL NO. OF HANDED OFF TRANSACTIONS (H) =', result[5])
                print ('TOTAL NO. OF PROCESSING TRANSACTIONS (W) =', result[6])
                print ('TOTAL NO. OF GENERATING TRANSACTIONS (G) =', result[7])
                print ('____________________________________________')
                print (' ')
                print (' ')



                for result in asia:
                    print ('    TOTAL NO. Asia TRANSCATIONS FOR TODAY =', result[1])
                    print ('TOTAL NO. OF FAILED TRANSACTIONS     (F) =', result[2])
                    print ('TOTAL NO. OF REPAIR TRANSACTIONS     (R) =', result[3])
                    print ('TOTAL NO. OF NOT GEN TRANSACTIONS    (N) =', result[4])
                    print ('TOTAL NO. OF HANDED OFF TRANSACTIONS (H) =', result[5])
                    print ('TOTAL NO. OF PROCESSING TRANSACTIONS (W) =', result[6])
                    print ('TOTAL NO. OF GENERATING TRANSACTIONS (G) =', result[7])
                    print ('____________________________________________')
                    print (' ')
                    print (' ')



        time.sleep(20)
        #os.system('clear')


    con1.close()
    con2.close()
    con3.close()
    con4.close()
    #os.system('clear')
