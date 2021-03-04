import requests
import json
import sqlite3
import pandas as pd
items={}
mylist=[]
df = pd.read_csv('interest.csv')
mylist = df['Interested_Group'].tolist()
# mylist = [x.lower() for x in mylist]
# class for reading json
class AjaxScraper:
    results = []

    def fetch(self, url):
        return requests.get(url)

    def parse(self, content):
        self.results = content['events']

        for result in self.results:
        

            event_name = result['name']
            event_start_date = result['start_date']
            event_end_date = result['end_date']
            event_start_time = result['start_time']
            event_end_time = result['end_time']
            event_timezone = result['timezone']
            event_url = result['url']
            event_ticket_url = result['tickets_url']
            items['event_name']=event_name
            items['timezone']=event_timezone
            items['start_date']=event_start_date
            items['end_date']=event_end_date
            items['start_time'] = event_start_time
            items['end_time'] = event_end_time
            items['event_url'] = event_url
            items['event_ticket_url'] = event_ticket_url

            topic = result['tags'][0:-1]
            tl = list(filter(lambda person: person['display_name'] in mylist, topic))

            if len(tl)!=0:
                items['interested'] = 'Yes'
            else:
                items['interested'] = 'No'
            
            sql.process_item(items)
    






    def run(self):
        response1 = self.fetch('https://www.eventbrite.com/api/v3/destination/events/?event_ids=138514174625,138537765185,135583021467,141588646453,139143519011,136253189959,139633546697,75827358671,136738443365,142701958399,140604209977,131765872271,142015434989,141852609975,133795402651,136787060781,138785269477,139255168959,138470953349,139250131893&expand=event_sales_status,primary_venue,image,saves,my_collections,ticket_availability&page_size=20')
        self.parse(response1.json())






class EventTaskPipeline(object):
    def __init__(self):
        self.create_connection()
        self.create_table()
        self.count=0
        
    def create_connection(self):
        self.conn = sqlite3.connect("Event_brite.db")
        self.curr = self.conn.cursor()
    def create_table(self):
        self.curr.execute(""" DROP TABLE IF EXISTS Event_brite """)
        self.curr.execute(""" DROP TABLE IF EXISTS Interested """)
        self.curr.execute(""" DROP TABLE IF EXISTS Non_Interested """)
        self.curr.execute(""" create table Event_brite(
                        event_name text,
                        timezone text,
                        start_date text,
                        end_date text,
                        start_time text,
                        end_time text,
                        event_url text,
                        event_ticket_url text)
                        """)
        self.curr.execute(""" create table Interested(
                                event_name text,
                                timezone text,
                                start_date text,
                                end_date text,
                                start_time text,
                                end_time text,
                                event_url text,
                                event_ticket_url text)
                                """)
        self.curr.execute(""" create table Non_Interested(
                                event_name text,
                                timezone text,
                                start_date text,
                                end_date text,
                                start_time text,
                                end_time text,
                                event_url text,
                                event_ticket_url text)
                                """)
    def process_item(self, item):
        self.store_db(item)
        print(item)
    def store_db(self,item):
        if self.count<10:
            self.curr.execute(""" insert into Event_brite values (?,?,?,?,?,?,?,?)""",(
                item['event_name'],
                item['timezone'],
                item['start_date'],
                item['end_date'],
                item['start_time'],
                item['end_time'],
                item['event_url'],
                item['event_ticket_url']
            ))
            self.count+=1
        if item['interested'] == 'Yes':
            self.curr.execute(""" insert into Interested values (?,?,?,?,?,?,?,?)""", (
                item['event_name'],
                item['timezone'],
                item['start_date'],
                item['end_date'],
                item['start_time'],
                item['end_time'],
                item['event_url'],
                item['event_ticket_url']
            ))
        else:
            self.curr.execute(""" insert into Non_Interested values (?,?,?,?,?,?,?,?)""", (
                item['event_name'],
                item['timezone'],
                item['start_date'],
                item['end_date'],
                item['start_time'],
                item['end_time'],
                item['event_url'],
                item['event_ticket_url']
            ))
        self.conn.commit()






if __name__ == '__main__':
    scraper = AjaxScraper()
    sql=EventTaskPipeline()
    scraper.run()
