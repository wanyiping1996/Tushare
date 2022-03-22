def get_data(pro,start_date,end_date):
    #上交所交易日期daily
    df_cal = pro.trade_cal(exchange='',start_date=start_date, end_date=end_date)#交易日历
    df_cal = df_cal.query('(exchange=="SSE") & (is_open==1)')#上交所&交易


    #交易日期weekly
    df_week = pd.read_csv('./year_week_index.csv')
    df_cal['cal_date'] = df_cal['cal_date'].astype('int64')
    df_cal['pretrade_date'] = df_cal['pretrade_date'].astype('int64')
    s = pd.Series(df_week['week'].values, pd.IntervalIndex.from_arrays(df_week['date_min'], df_week['date_max']))
    df_cal['week'] = df_cal['cal_date'].map(s)
    df_cal['year'] = df_cal['cal_date'].astype(str).str[:4].astype('int64')
    df_week = df_cal.groupby(['year', 'week']).max().reset_index()
    df_week.columns = ['year', 'week', 'exchange', 'cal_date', 'is_open', 'pretrade_date']
    df_week2 = pd.read_csv('./year_week_index.csv')
    df_week = pd.merge(df_week2, df_week, on=['year','week'], how='inner')
    return df_cal,df_week

# 计算SMB、HML weekly
def cal_x_weekly(pro,df_week):
    data_weekly = []
    data_st=pd.read_csv('df_st.csv')
    for idx, row in df_week.iterrows():
        date=str(row['cal_date'])
        if date=='20191231':
            df_weekly = pro.daily(trade_date=date)
        else:
            df_weekly = pro.weekly(trade_date=date)#周线行情
        df_basic = pro.daily_basic(trade_date=date)#每日指标
        df = pd.merge(df_weekly, df_basic, on='ts_code', how='inner')
        #排除ST股票ts_code,name,start_date,end_date,change_reason
        date_int=int(date)
        pd_st=data_st[(data_st['start_date']<=date_int) & (data_st['end_date']>=date_int)]
        df=df[~df['ts_code'].isin(pd_st['ts_code'].values)]
        #计算
        smb, hml, R_m = cal_smb_hml_weekly(df)
        year=str(row['year'])
        week=str(row['week'])
        date_min = str(row['date_min'])
        date_max = str(row['date_max'])
        data_weekly.append([date, smb, hml, R_m,year,week,date_min,date_max])
        print(date, smb, hml, R_m,year,week,date_min,date_max)
    df_tfm = pd.DataFrame(data_weekly, columns=['trade_date', 'SMB', 'HML', 'R_m','year','week','date_min','date_max'])
    # df_tfm['trade_date'] = pd.to_datetime(df_tfm.trade_date)
    # df_tfm = df_tfm.set_index('trade_date')
    return df_tfm
    
if __name__ == '__main__':
    # tushare初始化
    pro = ts.pro_api('xxx')
    # matplotlib设置
    plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
    plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
    start_date = '20170101'
    end_date = '20211231'

    # 获取日度数据、周度数据
    df_cal, df_week=get_data(pro,start_date,end_date)

    #上证50成分股
    # SZ50_codes=get_SZ50_stocks(start_date,end_date)
    # print(SZ50_codes)
    SZ50_codes=[600000,600028,600030,600031,600036,600048,600050,600104,
                600196,600276,600309,600436,600438,600519,600547,600570,
                600585,600588,600690,600745,600809,600837,600887,600893,
                600900,601012,601088,601166,601211,601288,601318,601336,
                601398,601601,601628,601633,601668,601688,601857,601888,
                601899,601919,603288,603986]
    ts_code=[]
    for i in SZ50_codes:
        ts_code.append(str(i)+'.SH')
        
    #计算周度三因子
    df_tfm=cal_x_weekly(pro,df_week)
    df_tfm.to_csv('df_three_factor_model_weekly.csv')
