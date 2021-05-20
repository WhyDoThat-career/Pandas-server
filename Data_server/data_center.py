#To Do
#데이터 센터에 쌓인 로그를 가져옵니다.
from connector import MySQL

class DataCenter :
    data_center = MySQL(key_file='keys/aws_dc_sql_key.json',database='career-center')

    @classmethod
    def get_raw_table(self,table) :
        return self.data_center.get_dataframe(table)

    @classmethod
    def get_click_count() :
        click_count = (self.data_center
                        .get_dataframe('click')
                        .group_by(['recruit_id'])
                        .count()
                        .user_id
                        .sort_values(ascending=False)
                        )
        return click_count
    
    @classmethod
    def get_popularity() :
        '''
        To Do
        북마크, 조회수 많은 순서로 데이터 프레임을 추출 (북마크가 더 높은 평가지수를 가짐)
        '''
        click_group = (self.data_center
                        .get_dataframe('click')
                        .group_by(['recruit_id'])
                        )

