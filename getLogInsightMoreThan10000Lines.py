import boto3
import datetime
import csv
import traceback
import time
def process_results(starttime, endtime):
    try:
        session = boto3.Session(profile_name='<ProfileName>')
        cw_client = session.client('logs')

        # select your interval, according to which you want to divide your time interval into
        interval = 7200
        # this is 120 mins in seconds

        count = 0
        with open('query' + '_results.csv', mode='a') as file:
            writer = csv.writer(file)
            periods = []
            writer.writerow(['name'])
            period_start = int(starttime)
            while period_start < endtime:
                period_end = min(period_start + int(interval), endtime)
                periods.append((period_start, period_end))
                
                logs = get_logs_for_query(cw_client, "", period_start, period_end)
                logs = logs['results']
                for log in logs:
                    writer.writerow([log[1]['value']])
                    count = count+1
                period_start = period_end
        print(count)

    except Exception as e:
        print("Something went wrong with account: " )
        print("Error: " + str(e))
        traceback.print_exc()

def get_logs_for_query(logs_client, query_str, start_time, end_time):

    query = "fields @timestamp, @message  | filter  strcontains(@message,'BillingController') and strcontains(@message,'Message') and strcontains(@message,'brand') and not strcontains(@message,'Error')"

    response = logs_client.start_query(logGroupName = "/aws/lambda/BillingNewPnrLambda",
                                      startTime=int(start_time),
                                      endTime=int(end_time),
                                      queryString=query,
                                      limit=10000
                                      )
    query_id = response['queryId']

    final_response = None

    while final_response is None or final_response['status'] == 'Running':
        print('Waiting for query to complete ...')
        time.sleep(1)
        final_response = logs_client.get_query_results(
            queryId=query_id
        )

    return final_response

def main():
    # start and endtime of the log insights query that you want to divide into smaller intervals
    starttime = 1727740800  # start of 1st
    endtime = 1730419199  # end of 31st
    process_results(starttime, endtime)



if __name__ == '__main__':
    main()