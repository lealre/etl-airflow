from  faker import Faker

fake = Faker()
Faker.seed(0)

def generate_companies_name(n:int = 3) -> list[str]:
    '''create fake companies '''
    companies_list = []
    for _ in range(n):
        companies_list(fake.company())
    
    return companies_list


data = {'company': [1,2,3],
        'currency': x,
        'value': x,
        'date': x}


