import pandas as pd
import numpy as np
import streamlit as st


class report:
    def __init__(self):
        self.agent_Data = None
        self.roaster_Data = None
        self.live_chat = None

    def loadingData(self,raw_agentFile,raw_roaster_File_data,raw_live_chat):
            self.raw_agentFile = raw_agentFile
            self.raw_roaster_File_data = raw_roaster_File_data
            self.raw_live_chat = raw_live_chat

            if self.raw_agentFile is not None:
                self.agent_Data_df = pd.ExcelFile(self.raw_agentFile, engine='openpyxl')
            else:
                st.error("Agent file not uploaded yet. Please upload the file before loading data.")
                return 
    

            if self.raw_roaster_File_data is not None:
                self.roaster_data_df = pd.ExcelFile(self.raw_roaster_File_data, engine='openpyxl')
            else:
                st.error("Agent file not uploaded yet. Please upload the file before loading data.")
                return 
            
            if self.raw_live_chat is not None:
                self.live_chat_df = pd.ExcelFile(self.raw_live_chat, engine='openpyxl')
            else:
                st.error("Agent file not uploaded yet. Please upload the file before loading data.")
                return 
            self.agent_Data = self.agent_Data_df.parse('Agent')
            self.roaster_Data = self.roaster_data_df.parse('Roaster')
            self.live_chat = self.live_chat_df.parse('Main File')
            
            self.cleaning_and_manipulation(self.agent_Data, self.roaster_Data, self.live_chat)
       
        

    def cleaning_and_manipulation(self,agent_Data,roaster_Data,live_chat):
        self.agent_Data = agent_Data
        self.roaster_Data = roaster_Data
        self.live_chat = live_chat

        # Removing the absent employees from the Roaster
        self.roaster_Data = self.roaster_Data[~(self.roaster_Data['present_attendance'] == 'WO')]
        # Removing the extra spaces from the column names
        self.agent_Data.columns = self.agent_Data.columns.str.strip()

        # selecting the Live chat as channel
        self.agent_Data = (self.agent_Data[self.agent_Data['Channel'] == 'live_chat'])

        # Extracting the Employee ID
        self.agent_Data["Emp_ID"] = self.agent_Data["Agent Email"].str.split('@').str[0]



        """  Joining the 2 data frames agent and roaster """


        self.agent_Data = self.agent_Data.merge(
        self.roaster_Data[['OLms ID','Emp Name','TL Name','Shift']],
        left_on = 'Emp_ID',
        right_on = 'OLms ID',
        how = 'left'
        )

        # Replacing the NaN values with Not Found 
        for i in ['OLms ID','Emp Name','TL Name','Shift']:
            self.agent_Data[i].fillna('Not Found',inplace=True)

        # Removing the Not Found """we can also use dropna here🤣 """
        self.agent_Data = self.agent_Data[~(self.agent_Data['OLms ID'] == 'Not Found')]
        self.agent_Data.reset_index(drop=True,inplace=True)

        # Removing the NULL values from live_chat 
        self.live_chat.dropna(subset=['Case First Response Time','Case First advisor assign time'],inplace = True)
        self.live_chat['FRT'] = self.live_chat['Case First Response Time'] - self.live_chat['Case First advisor assign time']

        # Removing the NULL values and calculating the AHT
        self.maxHours = pd.Timedelta(hours=5)
        self.live_chat.dropna(subset=['Case Last advisor assign time','Case Closure Time'], inplace= True)
        self.live_chat['AHT'] = self.live_chat['Case Closure Time'] - self.live_chat['Case Last advisor assign time']
        self.temp = self.live_chat['AHT'] < self.maxHours
        self.live_chat['temp'] = self.live_chat['AHT'] < self.maxHours
        self.live_chat = self.live_chat[~(self.live_chat['temp'] == False)]
        self.live_chat.drop(columns='temp',inplace=True)

        self.pivotTable_liveChat(self.agent_Data,self.live_chat)
    
    def pivotTable_liveChat(self,agent_Data,live_chat):
        self.agent_Data = agent_Data
        self.live_chat = live_chat
        # Creating the Pivot table
        self.live_chat_pivot = self.live_chat.pivot_table(
        index='Case First Assigned to Advisor',
        aggfunc={
            'FRT':[np.mean,'count'],
            'AHT': np.mean
        }
            )

        # Format mean of FRT as HH:MM:SS
        self.live_chat_pivot[('FRT', 'mean')] = self.live_chat_pivot[('FRT', 'mean')].astype(str).str.extract(r'(\d{2}:\d{2}:\d{2})')

        # Format mean of AHT as HH:MM:SS
        self.live_chat_pivot[('AHT', 'mean')] = self.live_chat_pivot[('AHT', 'mean')].astype(str).str.extract(r'(\d{2}:\d{2}:\d{2})')

        # Flatten the  Pivot Table
        self.live_chat_pivot.columns = [f"{i}_{j}" if j else i for i, j in self.live_chat_pivot.columns]

        # This moves the index into a column at the beginning of the DataFrame or you can say change index to regular column.``
        self.live_chat_pivot = self.live_chat_pivot.reset_index()

        self.merging_pivot_and_agent(self.agent_Data,self.live_chat)


    def merging_pivot_and_agent(self,agent_Data,live_chat):
        self.agent_Data = agent_Data
        self.live_chat = live_chat
        # Joining the agent_Data and live_chat_pivot
        self.agent_Data = self.agent_Data.merge(
            self.live_chat_pivot[['AHT_mean','FRT_count','FRT_mean','Case First Assigned to Advisor']],
            left_on= 'Emp Name',
            right_on= 'Case First Assigned to Advisor',
            how='left'  
        )

        # Dropping the Null values and resetting the index
        for i in ['AHT_mean','FRT_count','FRT_mean','Case First Assigned to Advisor']:
            self.agent_Data[i].fillna('Not Found',inplace=True)
        self.agent_Data = self.agent_Data[~(self.agent_Data['Case First Assigned to Advisor'] == 'Not Found')]
        self.agent_Data.reset_index(drop=True,inplace=True)

        """
        
        creating new columns in the data frame
        
        
        """
        self.agent_Data['Productive (Avail+Follow Up)'] = pd.to_timedelta(self.agent_Data['FOLLOW_UP']) + pd.to_timedelta(self.agent_Data['AVAILABLE'])
        self.agent_Data['Production (Avail+FollowUP+Tea+Lunch)'] = self.agent_Data['Productive (Avail+Follow Up)'] + pd.to_timedelta(self.agent_Data['LUNCH_BREAK']) + pd.to_timedelta(self.agent_Data['TEA_BREAK'])

        st.dataframe(self.agent_Data)





col1, col2, col3 = st.columns([0.1,4,1])
with col3:
    st.image('https://www.netimpactlimited.com/wp-content/uploads/2024/04/NetImpact-Logo-Final-Web-2.png')
with col2:
    st.subheader(f'Agent Performance Report😊')

# Agent file upload
raw_agentFile = st.file_uploader('Please upload the Agent file here 😎')
if raw_agentFile:
    st.success('file uploaded 😁')

# Roaster File Upload
raw_roaster_File_data = st.file_uploader(f'Please upload the Roaster file 😼')
if raw_roaster_File_data:
    st.success('File uploaded success 😍')        

# Live_chat file upload 
raw_live_chat = st.file_uploader(f'Please upload the live chat file 😎')
if raw_live_chat:
    st.success('Files received initiating the Automation HAIL NETIMPACT 🫡')

if raw_agentFile and raw_live_chat and raw_roaster_File_data is not None:
    rp = report()
    rp.loadingData(raw_agentFile,raw_roaster_File_data,raw_live_chat)
    # rp.cleaning_and_manipulation()
    # rp.pivotTable_liveChat()
    # rp.merging_pivot_and_agent()
    st.write(f'Have a good Day 😊 - Team data Analytics.')