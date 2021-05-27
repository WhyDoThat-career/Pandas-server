#To Do
#데이터 센터에 쌓인 로그를 가져옵니다.
from connector import MySQL
from pymysql.cursors import DictCursor

class DataCenter(MySQL) :
    def __init__(self) :
        super().__init__(key_file='keys/aws_dc_sql_key.json',database='career-center')

    @property
    def get_click(self) :
        return self.get_dataframe('click')

    @property
    def get_bookmark(self) :
        return self.get_dataframe('bookmark')
    
    @property
    def get_recruit_apply(self) :
        return self.get_dataframe('recruit_apply')

    @property
    def get_jobsector(self) :
        return self.get_dataframe('jobsector')
    
    @property
    def get_jobskill(self) :
        return self.get_dataframe('jobskill')

    @property
    def get_recruit_stack(self) :
        return self.get_dataframe('recruit_stack')

