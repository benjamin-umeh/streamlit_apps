import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import streamlit as st

# st.title('Remote Data Collection Quality Monitoring App')
st.markdown("<h1 style='text-align: center; color: grey;'>Remote Data Collection Quality Monitoring App</h1>", unsafe_allow_html=True)

lga_df = st.file_uploader("Upload the lga CSV with columns 'lga_name', 'lga_code', 'hh_samples'", type=['csv'])
enum_df = st.file_uploader("Upload the enumerators CSV with columns 'enumerator_name', 'enumerator_code', 'label'", type=['csv'])
df = st.file_uploader("Upload the survey data CSV", type=['csv'])


# start_date = datetime(2024, 8, 26, 00, 00, 00, 000000)
# end_data = datetime.now()

# d = st.date_input(
#     "Select the report date range",
#     (start_date, end_data)
#     )

yesterday = datetime.now() - timedelta(1)
yesterday = yesterday.date()

if df is not None and lga_df is not None and enum_df is not None:
     enum_df = pd.read_csv(enum_df)
     enum_df = enum_df.set_index('label')
     enum_df = enum_df[['enumerator_name', 'enumerator_code']]
     lga_df = pd.read_csv(lga_df)
     lga_df = lga_df[["lga_name","lga_code","hh_samples"]]

     df = pd.read_csv(df)
     df = df[pd.to_datetime(df['survey/start_survey/interview_start_time']).dt.date <= yesterday]
     

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
     full_enum_df = pd.concat([enum_df,gr_perf_by_enum], axis=1, join='inner')
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

     # Questionable submissions
     tot_count = df.groupby('survey/inf_id/enum_cod')['survey/start_survey/date_surv'].count()

     protein_ind_count_df = df.groupby(['survey/inf_id/enum_cod','survey/grp_fcs_gl/grp_fcs4_protein/FCSPr'])['survey/grp_fcs_gl/grp_fcs4_protein/FCSPr'].count()
     protein_ind_count_df.name = "count"
     protein_ind_count_df = pd.DataFrame(protein_ind_count_df)

     dairy_ind_count_df = df.groupby(['survey/inf_id/enum_cod','survey/grp_fcs_gl/grp_fcs3_milk/FCSDairy'])['survey/grp_fcs_gl/grp_fcs4_protein/FCSPr'].count()
     dairy_ind_count_df.name = "count"
     dairy_ind_count_df = pd.DataFrame(dairy_ind_count_df)

     pulses_ind_count_df = df.groupby(['survey/inf_id/enum_cod','survey/grp_fcs_gl/grp_fcs2_leg/FCSPulse'])['survey/grp_fcs_gl/grp_fcs4_protein/FCSPr'].count()
     pulses_ind_count_df.name = "count"
     pulses_ind_count_df = pd.DataFrame(pulses_ind_count_df)

     protein_ind_count_df = protein_ind_count_df.reset_index()
     protein_ind_count_df = protein_ind_count_df.set_index('survey/inf_id/enum_cod')

     dairy_ind_count_df = dairy_ind_count_df.reset_index()
     dairy_ind_count_df = dairy_ind_count_df.set_index('survey/inf_id/enum_cod')

     pulses_ind_count_df = pulses_ind_count_df.reset_index()
     pulses_ind_count_df = pulses_ind_count_df.set_index('survey/inf_id/enum_cod')

     protein_ind_count_df['survey/grp_fcs_gl/grp_fcs4_protein/FCSPr'] = protein_ind_count_df['survey/grp_fcs_gl/grp_fcs4_protein/FCSPr'].astype(int)
     dairy_ind_count_df['survey/grp_fcs_gl/grp_fcs3_milk/FCSDairy'] = dairy_ind_count_df['survey/grp_fcs_gl/grp_fcs3_milk/FCSDairy'].astype(int)
     pulses_ind_count_df['survey/grp_fcs_gl/grp_fcs2_leg/FCSPulse'] = pulses_ind_count_df['survey/grp_fcs_gl/grp_fcs2_leg/FCSPulse'].astype(int)

     mode_ind_protein = pd.DataFrame(protein_ind_count_df).join(pd.DataFrame(tot_count))
     mode_ind_protein['percent_occurred'] = mode_ind_protein['count']/mode_ind_protein['survey/start_survey/date_surv']
     mode_ind_protein["1st Observed Potential Anomaly"] = "About 70% of respondents from this enumerator reported consuming protein for " + mode_ind_protein['survey/grp_fcs_gl/grp_fcs4_protein/FCSPr'].astype(str) + " days in the last 7 days - This is questionable"
     mode_ind_protein = mode_ind_protein[mode_ind_protein['percent_occurred']>= 0.65]
     mode_ind_protein = pd.DataFrame(mode_ind_protein["1st Observed Potential Anomaly"])

     mode_ind_dairy = pd.DataFrame(dairy_ind_count_df).join(pd.DataFrame(tot_count))
     mode_ind_dairy['percent_occurred'] = mode_ind_dairy['count']/mode_ind_dairy['survey/start_survey/date_surv']
     mode_ind_dairy["2nd Observed Potential Anomaly"] = "About 70% of respondents from this enumerator reported consuming milk or other dairy products for " + mode_ind_dairy['survey/grp_fcs_gl/grp_fcs3_milk/FCSDairy'].astype(str) + " days in the last 7 days - This is questionable"
     mode_ind_dairy = mode_ind_dairy[mode_ind_dairy['percent_occurred']>= 0.65]
     mode_ind_dairy = pd.DataFrame(mode_ind_dairy["2nd Observed Potential Anomaly"])

     mode_ind_pulses = pd.DataFrame(pulses_ind_count_df).join(pd.DataFrame(tot_count))
     mode_ind_pulses['percent_occurred'] = mode_ind_pulses['count']/mode_ind_pulses['survey/start_survey/date_surv']
     mode_ind_pulses["3rd Observed Potential Anomaly"] = "About 70% of respondents from this enumerator reported consuming pulses for " + mode_ind_pulses['survey/grp_fcs_gl/grp_fcs2_leg/FCSPulse'].astype(str) + " days in the last 7 days - This is questionable"
     mode_ind_pulses = mode_ind_pulses[mode_ind_pulses['percent_occurred']>= 0.65]
     mode_ind_pulses = pd.DataFrame(mode_ind_pulses["3rd Observed Potential Anomaly"])

     full_anomaly_df = pd.concat([mode_ind_protein, mode_ind_dairy, mode_ind_pulses], axis=1)
     full_anomaly_df = full_anomaly_df.dropna(thresh=2)
     full_anomaly_df = pd.concat([enum_df, full_anomaly_df], axis=1, join="inner")

     # Final Performance Report
     tot_survey_lga = df.groupby(['survey/inf_id/a_lga'])['survey/inf_id/a_lga'].count()
     tot_survey_lga.name = 'Total Number of Households Surveyed'
     tot_survey_lga = pd.DataFrame(tot_survey_lga)

     final_perf_report = pd.concat([full_df,tot_survey_lga], axis=1, join='inner')
     final_perf_report = final_perf_report.rename(columns={'hh_samples':'Target Households Samples Size', 'valid_number_of_hhs_surveyed':'Total Valid Survey',\
                                                       'percentage_achieved':'Percentage of Valid Survey Achievement', 'lga_name':'LGA Name'})
     final_perf_report['Total Invalid Survey'] = final_perf_report['Total Number of Households Surveyed'] - final_perf_report['Total Valid Survey']
     final_perf_report["Percentage of Valid Survey Achievement"] = round(final_perf_report["Percentage of Valid Survey Achievement"],2)
     final_perf_report['LGA Name'] = final_perf_report['LGA Name'].str.upper() 

     unique_lga = enum_df_raw.lga.unique()

     new_udf = pd.DataFrame()
     enum_name_lists = []
     for u in unique_lga:
          udf = enum_df_raw[enum_df_raw['lga']==u]
          enum_names = list(udf.enumerator_name.values)
          enum_name_lists.append(enum_names)
     enum_name_lists 

     new_udf['lga'] = unique_lga
     new_udf['lga'] = new_udf['lga'].str.upper() 
     new_udf['Enumerators Names'] = enum_name_lists

     new_udf['Field Enumerators Names'] = new_udf['Enumerators Names'].apply(', '.join)
     new_udf = new_udf.drop(columns=['Enumerators Names'])
     new_udf = new_udf.set_index('lga')

     final_perf_report = final_perf_report.set_index("LGA Name")
     final_perf_report = pd.concat([final_perf_report,new_udf], axis=1, join='inner')
     final_perf_report = final_perf_report.reset_index()
     final_perf_report = final_perf_report.rename(columns={'index':"LGA Name"})
     final_perf_report = final_perf_report[["LGA Name", "Field Enumerators Names","Target Households Samples Size", "Total Number of Households Surveyed", "Total Invalid Survey","Total Valid Survey","Percentage of Valid Survey Achievement"]]

     

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
     

     full_anomaly_csv = full_anomaly_df.to_csv(index=True).encode('utf-8')
     st.download_button(
          label="Download Enumerators with Questionable Submissions CSV",
          data=full_anomaly_csv,
          file_name='questionable_submissions.csv',
          mime='text/csv',
      )

      final_perf_report = final_perf_report.to_csv(index=True).encode('utf-8')
     st.download_button(
          label="Download Final LGA/Enumerators Performance Report",
          data=final_perf_report,
          file_name='final_perf_report.csv',
          mime='text/csv',
      )

