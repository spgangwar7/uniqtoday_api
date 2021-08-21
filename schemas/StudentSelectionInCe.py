from pydantic import BaseModel
class StudentSelectionInCe(BaseModel):
    age:int
    gender:str
    school:str
    study_time:int
    no_of_attempts:int
    total_practice_tests_taken:int
    extra_school_classes:str
    extra_curriculars:str
    time_sm:int
    free_time:int
    go_out:int
    avg_perf_practice_tests:int
    travel_time:int
    city_name:str
    state_name:str


