''' Credentials (hidden) '''
host='?.?.?.?'
user = '???'
passwd = '???'

import pymysql

''' Function to retrieve the temperature information of a given state '''
def get_temp_info_of_state(state_input):
    db = pymysql.connect(host=host, user=user, passwd=passwd, db='temp_mf')
    cursor = db.cursor()    
    ''' Build query to get the row with the selected state '''
    sql_query = ''
    sql_query+=('SELECT * ')
    sql_query+=('FROM final ')
    sql_query+=('WHERE state = ' + '\'' + state_input + '\' ')
    ''' Run query and get result '''
    cursor.execute(sql_query)
    results = cursor.fetchone()
    ''' Close connection '''
    cursor.close()
    db.close()
    
    array_values = []
    for item in results:
        array_values.append(item)
    return array_values

''' Function the report string '''
def build_report_string(array_values):
    report = 'The minimum and maximum temperatures in {0} between 1990 and 2000 were respectively {1} and {2}.'\
    .format(array_values[1], array_values[2], array_values[3])    
    return report    

if __name__ == '__main__':
    ''' Get user input to select the state '''
    state_input = input("Enter a state (2 chars) ")
    state_input = state_input.upper()
    
    ''' Query database '''
    array_values = get_temp_info_of_state(state_input)
    
    ''' Print report '''
    print(build_report_string(array_values))