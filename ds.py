# Importing Libraries
import pandas as pd
import numpy as np
import streamlit as st

from streamlit_option_menu import option_menu

import io
from io import BytesIO
from pyxlsb import open_workbook as open_xlsb

import shutil
import os
import zipfile

import traceback

#Inserting Sidebar

with st.sidebar:
    selected=option_menu(
        menu_title='Menu',
        options=['Split dataset','Combine Dataset']
    )

if selected=='Split dataset':
   # Splitting the dataset
   st.title('Split the dataset')

   uploaded_file = st.file_uploader("Choose a file")

   if uploaded_file is not None:
      # To read file as bytes:
      try:
         #
         if 'csv' in uploaded_file.name:
            #
            df=pd.read_csv(uploaded_file,low_memory=False)


         elif '.xlsx' in uploaded_file.name:
            df=pd.read_excel(uploaded_file,engine='openpyxl')
            
         
      except Exception as e:
         print(traceback.format_exc())
         
         

      split_type = st.radio(
      "How do you want to split it by?",
      ["***Date***", "***Size***"],
      captions = ["For example, between 2024-01-01 to 2024-06-01","For example, create multiple files having lesser than 10 Lakh rows"],horizontal=True)
      
      if split_type=="***Date***":
         start_date = st.date_input("Select start date",format="YYYY-MM-DD")
         st.write('Your start date is',start_date)
         end_date = st.date_input("Select end date",format="YYYY-MM-DD")
         st.write('Your end date is',start_date)
         df['order_timestamp']=pd.to_datetime(df['email_timestamp'],format='mixed').dt.date
         df_new=df.loc[(df['order_timestamp']>=start_date)&(df['order_timestamp']<=end_date)]
         del df_new['order_timestamp']
         file_save=st.radio("Save the file as:",["***.csv***", "***.xlsx***"],horizontal=True)
         if file_save=='***.csv***':
            @st.cache_data
            def convert_df(df):
               #
               # IMPORTANT: Cache the conversion to prevent computation on every rerun
               return df.to_csv().encode('utf-8')

            csv = convert_df(df_new)
   
            st.download_button(
               label="Download data as CSV",
               data=csv,
               file_name='data.csv',
               mime='text/csv')
            
         elif file_save=='***.xlsx***':
            #
            @st.cache_data
            def to_excel(df):
               #
               output = BytesIO()
               writer = pd.ExcelWriter(output, engine='xlsxwriter')
               df.to_excel(writer, index=False, sheet_name='Sheet1')
               workbook = writer.book
               worksheet = writer.sheets['Sheet1']
               format1 = workbook.add_format({'num_format': '0.00'}) 
               worksheet.set_column('A:A', None, format1)  
               writer.close()
               processed_data = output.getvalue()
               return processed_data
            df_xlsx = to_excel(df_new)
            st.download_button(label='Download data as Excel file',data=df_xlsx ,file_name= 'df_test.xlsx')

      #Splitting by size
      elif split_type=="***Size***":
         # Split the DataFrame into chunks with max_rows rows each       
         # Button to trigger the zipping and download process
         file_save_1=st.radio("Save the file as:",["***.csv***", "***.xlsx***"],horizontal=True)
         
         def split_csv_excel_and_zip(input_df, output_prefix, max_rows=1000000):
            # Split the DataFrame into chunks with max_rows rows each
            chunks = [input_df[i:i + max_rows] for i in range(0, len(input_df), max_rows)]

            # Create a temporary directory to store individual CSV files
            output_dir = f"{output_prefix}_temp_files"
            os.makedirs(output_dir, exist_ok=True)

            # Save each chunk to a separate file
            

            
            for i, chunk in enumerate(chunks):
               if file_save_1=='***.csv***':
                  output_file = os.path.join(output_dir, f"{output_prefix}_part_{i + 1}.csv")
                  chunk.to_csv(output_file, index=False)
               else:
                  output_file = os.path.join(output_dir, f"{output_prefix}_part_{i + 1}.xlsx")
                  chunk.to_excel(output_file, index=False)
                  

            # Zip all CSV files into a single zip file
            zip_filename = f"{output_prefix}_output.zip"
            with zipfile.ZipFile(zip_filename, 'w') as zip_file:
               for root, dirs, files in os.walk(output_dir):
                     for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, output_dir)
                        zip_file.write(file_path, arcname=arcname)

            # Remove the temporary directory and all its contents if it exists
            if os.path.exists(output_dir):
               shutil.rmtree(output_dir)

            return zip_filename

         if uploaded_file is not None:
            if uploaded_file.size == 0:
               st.error("Error: The uploaded file is empty.")
            else:
               max_rows = st.slider("Maximum Rows per File", 1000, 100000, 100000, step=1000)
               output_prefix = st.text_input("Output File Prefix", "output_file_split")

               try:
                     input_df = df
                     if not input_df.empty:
                           zip_filename = split_csv_excel_and_zip(input_df, output_prefix, max_rows)
                           with open(zip_filename, 'rb') as f:
                              st.download_button('Download Zip', f, file_name='archive.zip')
                     else:
                        st.error("Error: The uploaded file does not contain any data.")
               except pd.errors.EmptyDataError:
                     st.error("Error: The uploaded file could not be parsed. Please make sure it contains data.")
elif selected=='Combine Dataset':
   st.title('Combine Dataset')
   def combine_and_save_files(uploaded_files):
    ultimate_df = pd.DataFrame()

    encodings_to_try = ['utf-8', 'latin1','cp1252','utf-16']

    for file in uploaded_files:
        if file.name.endswith('.csv'):
            successful_read = False
            for encoding in encodings_to_try:
                try:
                    df = pd.read_csv(file, low_memory=False, encoding=encoding)
                    ultimate_df = pd.concat([df, ultimate_df])
                    successful_read = True
                    break  # Break out of the loop if reading succeeds with the current encoding
                except UnicodeDecodeError:
                    pass  # Try the next encoding in case of a UnicodeDecodeError

            if not successful_read:
                st.error(f"Error reading CSV file {file.name}. None of the encodings worked.")

        elif file.name.endswith(('.xls', '.xlsx')):
            df = pd.read_excel(file, engine='openpyxl')
            ultimate_df = pd.concat([df, ultimate_df])

    return ultimate_df
   
   uploaded_files = st.file_uploader("Choose files", type=["csv", "xlsx", "xls"], accept_multiple_files=True)

   if uploaded_files:
      # st.write(f"Selected files: {[file.name for file in uploaded_files]}")
      combine_button = st.button('Combine Files')

      if combine_button:
         try:
               combined_df = combine_and_save_files(uploaded_files)

               save_as = st.radio("Save the file as:", [".csv", ".xlsx"])

               if save_as == ".csv":
                  csv_data = combined_df.to_csv(index=False).encode('utf-8')
                  st.download_button("Download data as CSV", data=csv_data, file_name='combined_data.csv')

               elif save_as == ".xlsx":
                  excel_data = io.BytesIO()
                  with pd.ExcelWriter(excel_data, engine='xlsxwriter') as writer:
                     combined_df.to_excel(writer, index=False, sheet_name='Sheet1')
                  st.download_button("Download data as Excel file", excel_data.getvalue(), file_name='combined_data.xlsx')

         except Exception as e:
               st.error(f"An error occurred: {e}")
         
         
            




            

            
            


            



        
   


    


    
   