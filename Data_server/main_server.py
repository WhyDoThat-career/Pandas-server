#To Do
#메인 서버로부터 데이터를 가져 옵니다.
from connector import MySQL

class MainServer(MySQL) :
    def __init__(self) :
        super().__init__(key_file='keys/aws_sql_key.json',database='crawl_job')

    @property
    def get_recruit_data(self) :
        return self.get_dataframe('job_detail')

    @property
    def get_jobplanet_data(self) :
        return self.get_dataframe('company_info')

    
