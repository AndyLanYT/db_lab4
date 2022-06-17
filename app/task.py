import pymongo
import csv
import time



print('here')
def mark(string):
    if string != 'null' and string is not None:
        return float(string.replace(',', '.'))

ZNO2019 = 'Odata2019File.csv'
ZNO2021 = 'Odata2021File.csv'

create = '''
CREATE TABLE IF NOT EXISTS tbl_ZNO (
    outID VARCHAR (36) PRIMARY KEY,
    birth INT NOT NULL,
    sexType VARCHAR (255) NOT NULL,
    regname VARCHAR (255),
    testName VARCHAR (255) NOT NULL,
    testMark REAL,
    testStatus BOOLEAN,
    year INT NOT NULL
);
'''


tries = 5
while tries:
    try:
        db_client = pymongo.MongoClient('mongodb://admin:password@mongodb')

        db = db_client['znodata']
        collection = db['zno']
        

        start = time.time()
        
        count = collection.count_documents({'year': 2019})
        print(count)

        # with open(ZNO2019, 'r', encoding='windows-1251') as csvfile:
        #     reader = csv.reader(csvfile, delimiter=';')
        #     headers = next(reader)

        #     outid = headers.index('OUTID')
        #     birth = headers.index('Birth')
        #     sextypename = headers.index('SEXTYPENAME')
        #     regname = headers.index('REGNAME')
        #     engTest = headers.index('engTest')
        #     engBall100 = headers.index('engBall100')
        #     engTestStatus = headers.index('engTestStatus')
        #     year = 2019

        #     for idx in range(count):
        #         if idx % 10000 == 0:
        #             print(f'{idx} records skipped!')
                
        #         next(reader)
            
        #     for idx, row in enumerate(reader):
        #         values = {'outID': row[outid],
        #                   'birth': row[birth],
        #                   'sexTypeName': row[sextypename],
        #                   'regName': row[regname],
        #                   'testName': row[engTest],
        #                   'testMark': mark(row[engBall100]),
        #                   'testStatus': row[engTestStatus] != 'null',
        #                   'year': year}
                
        #         collection.insert_one(values)

        #         if idx % 10000 == 0:
        #             print(f'{idx} records added!')


        # count = collection.count_documents({'year': 2021})
        # print(count)
    
        # with open(ZNO2021, 'r', encoding='utf-8-sig') as csvfile:
        #     reader = csv.reader(csvfile, delimiter=';')
        #     headers = next(reader)

        #     outid = headers.index('OUTID')
        #     birth = headers.index('Birth')
        #     sextypename = headers.index('SexTypeName')
        #     regname = headers.index('RegName')
        #     engTest = headers.index('EngTest')
        #     engBall100 = headers.index('EngBall100')
        #     engTestStatus = headers.index('EngTestStatus')
        #     year = 2021

        #     for idx in range(count):
        #         if idx % 10000 == 0:
        #             print(f'{idx} records skipped!')

        #         next(reader)
            
        #     for idx, row in enumerate(reader):
        #         values = (row[outid], row[birth], row[sextypename], row[regname], mark(row[engBall100]), row[engTestStatus] != 'null', 2021)
        #         values = {'outID': row[outid],
        #                   'birth': row[birth],
        #                   'sexTypeName': row[sextypename],
        #                   'regName': row[regname],
        #                   'testName': row[engTest],
        #                   'testMark': mark(row[engBall100]),
        #                   'testStatus': row[engTestStatus] != 'null',
        #                   'year': year}
                
        #         collection.insert_one(values)

        #         if idx % 10000 == 0:
        #             print(f'{idx} records added!')
        
        print('All data successfuly inserted')

        with open('execution time.txt', 'w') as timefile:
            timefile.write(f'Execution time: {time.time() - start}')
            print(f'Execution time: {time.time() - start}')

        
        pipeline = [
            {'$match': {'testStatus': True}},
            {'$group': {
                '_id': {
                    'regName': '$regName',
                    'year': '$year'
                },
                'maxMark': {'$max': '$testMark'}
            }},
            {'$sort': {'_id.regName': 1}}
        ]
        result = collection.aggregate(pipeline)


        with open('ZNOdata.csv', 'w', newline='', encoding='utf-8') as csvfile:
            maxMarks2019 = []
            maxMarks2021 = []
            for el in result:
                regName = el['_id']['regName'] 
                year = el['_id']['year']
                maxMark = el['maxMark']

                if year == 2019:
                    maxMarks2019.append((regName, maxMark))
                elif year == 2021:
                    maxMarks2021.append((regName, maxMark))

            writer = csv.writer(csvfile, delimiter=';')
            writer.writerow(['regName', 'maxMark2019', 'maxMark2021'])
            
            for i in range(len(maxMarks2019)):
                writer.writerow([maxMarks2019[i][0], maxMarks2019[i][1], maxMarks2021[i][1]])
        
        print('Created file ZNOdata.csv with statistics')
        
        tries = 0

    except FileNotFoundError as err:
        tries = 0
        # print('FileNotFoundError')
        print(f'File {err.filename} does not exist')

    except:
        print('Undefined error!!')
