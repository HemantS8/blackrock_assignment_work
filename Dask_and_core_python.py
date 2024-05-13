#!/usr/bin/env python
# coding: utf-8
import dask.dataframe as dd
import os,sys,glob
from datetime import datetime
import LogConfiguration
from configparser import ConfigParser
import traceback

class data_quality_check:
    Filename_Config = r'properties\Quality_Check_Config.ini'
    def __init__(self):
        try:
            #configuration to read file
            config = ConfigParser()
            config.read(self.Filename_Config)     
            self.file_log = 'data_quality_check_using_dask'
            self.dir_logs = config['path']['dir_logs']
            self.dir_input = config['path']['dir_input']
            #configuring logs
            Log = LogConfiguration.LogConfiguration(os.path.join(self.dir_logs, self.file_log) + '_' + datetime.now().strftime('%Y%m%d_%H%M') + '.log' , self.file_log) 
            self.logger = Log.getLog()
            self.logger.info('log setup completed successfully')
        except Exception as e:
            self.logger.error('Error while setting up log and path:' + str(e))
            trace_info = traceback.format_exc()
            self.logger.error(trace_info)
            sys.exit(-1)

    def extract_data_from_file(self):
        try:
            #reading the input file
            input_file = glob.glob(os.path.join(self.dir_input, 'stocks_df') + '.csv')
            #reading to dataframe
            self.df = dd.read_csv(input_file[0], assume_missing=True)
            self.df = self.df.repartition(npartitions=5)
            self.logger.info('extract data completed successfully')
        except Exception as e:
            self.logger.error('Error while extracting data from file:' + str(e))
            trace_info = traceback.format_exc()
            self.logger.error(trace_info)
            sys.exit(-1)

    def check_date_format(self, date_str):
        try:
            date_obj = datetime.strptime(date_str, '%m/%d/%Y')
            return date_obj <= datetime.now()
        except ValueError:
            return False

    def date_validity_check(self):
        # Convert date column to datetime format and check for valid dates
        try:
            invalid_dates = self.df[~self.df['Date'].apply(self.check_date_format, meta=('Date', 'bool'))]
            invalid_dates = invalid_dates.compute()
            invalid_dates.to_csv('QC_failures/1_qc_failure_dates.csv.csv', index=False)
            self.logger.info('date format check completed successfully')
        except Exception as e:
            self.logger.error("Error during date format check:" + str(e))
            trace_info = traceback.format_exc()
            self.logger.error(trace_info)

    def check_missing_and_negative_values(self):
        try:
            #check for missing values 
            missing_values = self.df[['Open', 'High', 'Close', 'Low', 'Volume']].isna()
            if len(missing_values) > 0:
                missing_values = self.df[missing_values.any(axis=1)].compute()
                missing_values.to_csv('QC_failures/2_missing_values.csv', index=False)
            
            #Check for negative in Open, High, Low, Close, and Volume columns
            negative_values = self.df[(self.df[['Open', 'High', 'Low', 'Close', 'Volume']] < 0).any(axis=1)]
            if len(negative_values) > 0:
                negative_values = negative_values.compute()
                negative_values.to_csv('QC_failures/2_negative_values.csv', index=False)
            self.logger.info('check missing and negative values completed successfully')
        except Exception as e:
            self.logger.error("Error in check missing and negative values:" + str(e))
            trace_info = traceback.format_exc()
            self.logger.error(trace_info)

    def check_change_percent(self):
        try:
            # Check for change percent values not rounded to two decimal places
            incorrect_change_percent = self.df[~(self.df['Change Percent'].round(2) == self.df['Change Percent'])]
            if len(incorrect_change_percent) > 0:
                incorrect_change_percent = incorrect_change_percent.compute()
                incorrect_change_percent.to_csv('QC_failures/3_incorrect_decimals_in_change_percent.csv.csv', index=False)
        except Exception as e:
            self.logger.error("Error in check_change_percent", str(e))
            trace_info = traceback.format_exc()
            self.logger.error(trace_info)

    def outcome_value_validity_check(self):
        try:
            # Filter rows where 'Change Percent' is not null
            filtered_df = self.df[self.df['Change Percent'].notnull()]
            
            # Filter rows where 'Outcome' is not 'PROFIT' or 'LOSS'
            invalid_outcome = filtered_df[~filtered_df['Outcome'].isin(['PROFIT', 'LOSS'])]
            invalid_outcome.to_csv('QC_failures/4_invalid_outcome_values.csv.csv', single_file=True, index=False)
            self.logger.info('outcome value validity check completed successfully')
            
        except Exception as e:
            self.logger.error("Error in outcome_value_validity_check" + str(e))
            trace_info = traceback.format_exc()
            self.logger.error(trace_info)

    def check_correctness_of_outcome(self):
        try:
            # Check if Outcome value is correct based on Close and Open prices
            incorrect_outcome = self.df[((self.df['Close'] > self.df['Open']) & (self.df['Outcome'] != 'PROFIT')) |
                                        ((self.df['Close'] < self.df['Open']) & (self.df['Outcome'] != 'LOSS'))]
            if len(incorrect_outcome) > 0:
                incorrect_outcome = incorrect_outcome.compute()
                incorrect_outcome.to_csv('QC_failures/5_incorrect_outcome.csv',index=False)
            self.logger.info('check correctness of outcome completed sucessfully')
        
        except Exception as e:
            self.logger.error("Error in check_correctness_of_outcome:" + str(e))
            trace_info = traceback.format_exc()
            self.logger.error(trace_info)

if __name__ == "__main__":
    dqc = data_quality_check()
    dqc.extract_data_from_file()
    dqc.date_validity_check()
    dqc.check_missing_and_negative_values()
    dqc.check_change_percent()
    dqc.outcome_value_validity_check()
    dqc.check_correctness_of_outcome()