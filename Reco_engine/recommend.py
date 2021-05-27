#To Do
#추천 엔진을 구현합니다.
from sklearn.metrics.pairwise import cosine_similarity
from Data_server.main_server import MainServer
from Data_server.data_center import DataCenter
import pandas as pd
from datetime import datetime, timedelta

class Recommend :
    def __init__(self) :
        self.main_db = MainServer()
        self.dc_db = DataCenter()
        self.job_detail = self.main_db.get_recruit_data
        self.click = self.dc_db.get_click

    def recommend(self,user_id) :
        self.click = self.dc_db.get_click
        self.job_detail = self.main_db.get_recruit_data
        user_click = self.click[self.click['user_id']==user_id]['recruit_id']

        if len(user_click) == 0 :
            top10 = self.non_personal()
        else :
            top10 = self.personal(user_click)
        return top10


    def non_personal(self) :
        '''
        비개인화 추천

        비개인화 추천에는 다음요소가 포함된다.
        - 클릭수
        - 북마크 수
        - 지원 수
        '''
        bookmark = self.dc_db.get_bookmark
        apply = self.dc_db.get_recruit_apply

        total_count = pd.conat([self.click,pd.concat([bookmark,apply])])

        top10 = (total_count
                    .groupby(['recruit_id'])
                    .count()
                    .sort_values(by='user_id',ascending=False)[:10])
        
        return list(top10.index)
    
    def personal(self,user_click) :
        '''
        개인화 추천

        개인화 추천에는 다음 요소가 포함된다.
        - 사용자가 이력서에 입력한 기술직군
        - 사용자가 이력서에 입력한 기술스택
        - 사용자가 클릭한 공고의 직군
        - 사용자가 클릭한 공고의 스택
        - 최근 30일 이내의 공고
        - 사용자가 한번도 클릭하지 않은 공고
        '''
        set_job = self.job_detail.set_index('id')

        cos_df = self.get_similarity_dataframe()
        filter_click = [recruit for recruit in user_click if recruit in cos_df.index]
        ranking = (cos_df[filter_click][set_job['crawl_date'] > datetime.now()-timedelta(days=30)]
                            .mean(axis=1)
                            .sort_values(ascending=False))
        unviewed_top10 = ranking.drop([recruit for recruit in filter_click if recruit in ranking.index])[:10]
        return list(unviewed_top10.index)

    def get_similarity_dataframe(self) :
        sector = self.dc_db.get_jobsector
        skill_tag = self.dc_db.get_recruit_stack
        recruit_skill = dict()
        all_tag = self.get_all_tags(sector,skill_tag)

        for rid,skill in all_tag.values :
            if rid in recruit_skill :
                recruit_skill[rid][int(skill)-1] = 1
            else :
                recruit_skill[rid] = [0 for i in range(326)]
        
        df = pd.DataFrame(recruit_skill)
        cos_df = pd.DataFrame(cosine_similarity(df.T), columns = df.columns)
        cos_df.insert(0,"Columns",df.columns)
        cos_df = cos_df.set_index('Columns')

        return cos_df
    
    def get_all_tags(self,sector,skill_tag) :
        sector['id'] = sector['id']+300
        sector_tag = (pd.merge(self.job_detail[['id','sector']],sector,
                                 left_on='sector',
                                 right_on='name')[['id_x','id_y']]
                        .rename(columns={'id_x':'recruit_id','id_y':'skill_id'}))

        return pd.concat([skill_tag,sector_tag])[['recruit_id','skill_id']]