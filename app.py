import pandas as pd
import numpy as np
from datetime import datetime
import streamlit as st

st.title('Data Collection Monitoring App')

lga_df = pd.read_csv('lga_target.csv') #st.file_uploader("Upload the lga CSV with columns lga_name, lga_code, hh_samples", type=['csv'])
enum_df = pd.read_csv('enum_df.csv')
enum_df = enum_df.set_index('label')
df = st.file_uploader("Upload the survey data CSV", type=['csv'])

if df is not None:
     df = pd.read_csv(df)

     # lga_df = pd.read_csv('lga_target.csv', engine='python')
     lga_df = lga_df.set_index('lga_code')
     # df = pd.read_csv('data.csv', engine='python')
     df = df[df['survey/interview_end_time'].notna()]

     df['survey/start_survey/interview_start_time'] = pd.to_datetime(df['survey/start_survey/interview_start_time'])
     df['survey/interview_end_time'] = pd.to_datetime(df['survey/interview_end_time'])


     grouped_df = df.groupby(['survey/inf_id/a_lga','enum_name','survey/inf_id/enum_cod', 'survey/start_survey/date_surv','_uuid'])[['survey/start_survey/interview_start_time','survey/interview_end_time']].mean()
     grouped_df['survey/interview_end_time'] = grouped_df['survey/interview_end_time'].values.astype(np.int64) // 10 ** 9
     grouped_df = grouped_df.sort_values(by=['survey/inf_id/enum_cod', 'survey/start_survey/interview_start_time'])
     grouped_df['survey/start_survey/interview_start_time'] = grouped_df['survey/start_survey/interview_start_time'].values.astype(np.int64) // 10 ** 9

     grouped_df['currrent_survey_time (minutes)'] = (grouped_df['survey/interview_end_time'] - grouped_df['survey/start_survey/interview_start_time'])/60
     grouped_df['prev_survey_time (minutes)'] = grouped_df.groupby('survey/inf_id/enum_cod')['currrent_survey_time (minutes)'].shift(1)
     
     
     grouped_df['prev_survey_end_time'] = grouped_df.groupby('survey/inf_id/enum_cod')['survey/interview_end_time'].shift(1)
     grouped_df['survey_validity'] = np.where((grouped_df['currrent_survey_time (minutes)']<=(80*0.25)) | (grouped_df['survey/start_survey/interview_start_time'] < grouped_df['prev_survey_end_time']), 'Invalid', 'Valid')
     
     enum_perf_df = grouped_df.copy()
     
     perf_by_enum = enum_perf_df.copy()
     
     gr_perf_by_enum = perf_by_enum.groupby(['survey/inf_id/enum_cod', 'survey_validity'])['survey_validity'].count()
     gr_perf_by_enum.name = "valid_number_of_hhs_surveyed"
     gr_perf_by_enum = gr_perf_by_enum.reset_index()
     gr_perf_by_enum = gr_perf_by_enum.set_index('survey/inf_id/enum_cod')
     
     gr_perf_by_enum = gr_perf_by_enum[gr_perf_by_enum['survey_validity']=='Valid']
     gr_perf_by_enum = gr_perf_by_enum.drop(columns=['survey_validity'])
     full_enum_df = pd.concat([enum_df,gr_perf_by_enum], axis=1)
     full_enum_df = full_enum_df.fillna(0)
     full_enum_df['valid_number_of_hhs_surveyed'] = full_enum_df['valid_number_of_hhs_surveyed'].astype('int')

     grouped_df['Remark'] = np.where(grouped_df['currrent_survey_time (minutes)']<=(80*0.25), 'Interview Time Too Short - Not Realistic', '')
     grouped_df['current_survey_started_before_end_of_previous_survey'] = grouped_df['survey/start_survey/interview_start_time'] < grouped_df['prev_survey_end_time']
     grouped_df['Remark'] = np.where(((grouped_df['Remark']=='') & (grouped_df['current_survey_started_before_end_of_previous_survey'])), 'Started Multiple interviews almost at the same time', grouped_df['Remark'])
     full_report = grouped_df.copy()
     full_report = full_report.reset_index()
     full_report = full_report.set_index('survey/inf_id/enum_cod')
     full_report = full_report.join(enum_df)
     # full_reort = pd.concat([full_reort, enum_df], axis=1)
     full_report = full_report[full_report['survey_validity'] == 'Invalid']
     full_report = full_report[['enumerator_name', 'enumerator_code', 'currrent_survey_time (minutes)', 'survey_validity', 'Remark' ]]
     full_report = full_report.sort_values('enumerator_name')
     

     perf_by_lga = grouped_df.reset_index()#(names=['survey/inf_id/enum_cod', 'survey/start_survey/date_surv', '_uuid'])
     gr_perf_by_lga = perf_by_lga.groupby(['survey/inf_id/a_lga', 'survey_validity'])['survey_validity'].count()
     gr_perf_by_lga.name = "valid_number_of_hhs_surveyed"
     gr_perf_by_lga = gr_perf_by_lga.reset_index()
     gr_perf_by_lga = gr_perf_by_lga.set_index('survey/inf_id/a_lga')

     gr_perf_by_lga = gr_perf_by_lga[gr_perf_by_lga['survey_validity']=='Valid']
     gr_perf_by_lga = gr_perf_by_lga.drop(columns=['survey_validity'])


     full_df = pd.concat([lga_df, gr_perf_by_lga], axis=1)
     full_df = full_df.dropna()
     full_df['percentage_achieved'] = (full_df['valid_number_of_hhs_surveyed']/full_df['hh_samples'])*100

     full_report_csv = full_report.to_csv(index=True).encode('utf-8')
     st.download_button(
          label="Download Invalid Report by Enumerators CSV",
          data=full_report_csv,
          file_name='invalid_surveys_report.csv',
          mime='text/csv',
      )


     enum_perf_csv = full_enum_df.to_csv(index=True).encode('utf-8')
     st.download_button(
          label="Download Summary Performance by Enumerators CSV",
          data=enum_perf_csv,
          file_name='summary_enum_perf.csv',
          mime='text/csv',
      )

     lga_perf_csv = full_df.to_csv(index=True).encode('utf-8')
     st.download_button(
          label="Download Summary Performance by LGA CSV",
          data=lga_perf_csv,
          file_name='summary_lga_perf.csv',
          mime='text/csv',
      )
