import pandas as pd
from datetime import datetime
from datetime import date
import pyodbc

DMonth={
		    "January" : '01' ,
	    	"February" : '02' ,
	    	"March" : '03' ,
	    	"April" : '04' ,
            "May" : '05'	,
            "June" : '06' ,
            "July" : '07' ,
	    	"August" : '08' ,
	    	"September" : '09' ,
	    	"October" : '10' ,
           "November" : '11',
           "December" : '12'  
	       }


filepath=r'C:/Users/70018928/Documents/Project2020/MAC/Test_ReadCube/CUBE_CVS.xlsx'
# EYC
def dataprep(sheetname, filepath):
    df_dummy=pd.read_excel(filepath,sheet_name=sheetname)
    #print(' columns : ',df_dummy.columns)
    #print(' == ',df_dummy['Unnamed: 1'][4], ' == ',DMonth[df_dummy['Unnamed: 1'][7]])
    
    if(sheetname=='EYC'):
        Year_Month=df_dummy['Unnamed: 1'][4]+'-'+DMonth[df_dummy['Unnamed: 1'][7]]
        df_data=df_dummy[12:].copy().reset_index()
        file_path =r'C:\Users\70018928\Documents\Project2020\MAC\Test_ReadCube\Output_EYC.csv'
    else:
        Year_Month=df_dummy['Unnamed: 1'][2]+'-'+DMonth[df_dummy['Unnamed: 1'][4]]
        df_data=df_dummy[9:].copy().reset_index()
        file_path =r'C:\Users\70018928\Documents\Project2020\MAC\Test_ReadCube\Output_DHB.csv'
    print(' ==> ', Year_Month)

    df_data.columns=['index','Store_Label','ProductDHB_Manufacturer','ProductDHB_MainBrand','ProductDHB_SubBrand','ProductDHB_Format',
        'ProductDHB_Size','ProductDHB_PackSize', 'Volumn','NaN']

    df_data1=df_data.drop(columns=['index','NaN'],axis=1).reset_index()
    df_data1['Year-Month']=Year_Month

    print(' === > ',df_data1.columns)
    #print(' :: ',df_data1)

    colIndex=df_data1['index'].values.tolist()
    bizList=[]
    #print(' :: == ',len(colIndex))
    for n in colIndex:
        business=df_data1.loc[df_data1['index']==n,'ProductDHB_Manufacturer'].tolist()
        #print(' ===>',business[0],'== ',type(business[0]))
        if(business[0]=='Thai Bev'):
            output='Thaibev'
        elif (business[0]=='Boonrawd'):
            output='Boonrawd'
        else:            
            output='Other'
        bizList.append(output)
        #print(' ===> ',business,' == ',type(business), ' ==== ', output)

    dfBiz=pd.DataFrame(bizList)
    dfBiz.columns=['Business']
    df_data1=pd.concat([df_data1,dfBiz],axis=1)
    df_data1=df_data1.drop(columns=['index'],axis=1)
    df_data1.to_csv(file_path)
    return df_data1, Year_Month


def WriteToDataBase_SendList(df_input):

    df_write=df_input.replace(np.nan,-999)

    conn1 = pyodbc.connect('Driver={SQL Server};'
                        'Server=SBNDCBIDSCIDB01;'
                        'Database=TBL_ADHOC;'
                        'Trusted_Connection=yes;')

    #- Delete all records from the table
    sql="""DELETE FROM TBL_ADHOC.dbo.TB_AGSendLog"""

    cursor=conn1.cursor()
    cursor.execute(sql)
    conn1.commit()

    for index, row in df_write.iterrows():
        cursor.execute("INSERT INTO TBL_ADHOC.dbo.TB_AGSendLog([Region],[Province],[Sender],[Group],[Agent Code],[Agent Name],[Send_Flag],[Send_Flag_1],[Send_Flag_2],[Noti_Message],[QD],[Stat_Msg]) values(?,?,?,?,?,?,?,?,?,?,?,?)", row['Region'],row['Province'],row['Sender'],row['Group'],row['Agent Code'],row['Agent Name'],row['Send_Flag'],row['Send_Flag_1'],row['Send_Flag_2'],row['Noti_Message'],row['QD'],row['Stat_Msg'] )

    conn1.commit()

    cursor.close()
    conn1.close()

    print(' complete WritetoDB Sendlist')




dfEYC, YM_EYC=dataprep('EYC', filepath)
dfDHB, YM_DHB=dataprep('DHB', filepath)

# Check if both comes from the same Year_Month
if(YM_EYC == YM_DHB):
    print(' ---------- Proceed --------------')
    dfAll=dfEYC.append(dfDHB).reset_index()
    dfAll=dfAll.drop(columns=['index'],axis=1)

    #print(dfALL)
    file_path =r'C:\Users\70018928\Documents\Project2020\MAC\Test_ReadCube\Output_ALL.csv'
    dfAll.to_csv(file_path)
else:
    print('------------Check*****************')


