from __future__ import print_function
import sys
import re
import io
import numpy
import collections
import os
import pandas
from apiclient.http import MediaIoBaseDownload
from apiclient import discovery
from httplib2 import Http
import xlsxwriter
from oauth2client import file, client, tools
import json
from md_scripts import md

class MyWriter:
    def __init__(self, stdout, filename):
    	self.old_stdout=sys.stdout #save stdout
    	self.stdout = stdout
    	self.logfile = open(filename, 'w', encoding="utf-8")

    def write(self, text):
    	self.stdout.write(text)
    	self.logfile.write(text)

    def close(self):
    	sys.stdout = self.old_stdout #restore normal stdout and print
    	self.logfile.close()

    def flush(self):
    	pass


class taxonomy_qa(md.dcm_qa):
	def __init__(self, tempfolder):
		# self.writer = MyWriter(sys.stdout, str(tempfolder)+"/results.txt")
		# sys.stdout = self.writer		
		self.logstring=""
		md.dcm_qa.__init__(self)
		debug = False
		self.logtxt=""
		self.tempfolder = tempfolder
		self.SCOPES = 'https://www.googleapis.com/auth/drive.readonly'
		self.prog_taxonomy = {}
		self.social_taxonomy = {}		
		self.direct_taxonomy = {}
		self.search_taxonomy = {}
		self.inapp_taxonomy = {}
		self.ott_taxonomy = {}
		self.performance_taxonomy = {}
		self.get_last_checked_placement_id()
		self.get_last_checked_creative_id()
		self.get_placement_taxonomy_file()
		self.extract_creative_taxonomy()
		self.extract_taxonomy()
		self.new_last_placement_id=0
		self.new_last_creative_id=0
		self.bad_placements=[]
		self.bad_creatives=[]
		self.master_errors_list={}
		self.master_errors_list['placement_errors'] = collections.defaultdict(list)
		self.master_errors_list['creative_errors'] = collections.defaultdict(list)

	def generate_results_spreadsheet(self):
		if len(self.bad_placements) + len(self.bad_creatives) == 0:
			return False
		output_array=[]
		output_array.append(['type','channel','id','name','errors'])
		for placement in self.bad_placements:
			output_row = []
			output_row.extend(['placement', placement['channel'], placement['id'],placement['name']])
			for error in placement['errors']:
				output_row.append(json.dumps(error))
			output_array.append(output_row)
		for creative in self.bad_creatives:
			output_row = []
			output_row.extend(['creative', "N/A", creative['id'],creative['name']])
			for error in creative['errors']:
				output_row.append(json.dumps(error))
			output_array.append(output_row)			

		workbook = xlsxwriter.Workbook('taxonomy_violations.xlsx')
		worksheet = workbook.add_worksheet()

		rownum = -1
		for row in output_array:
			colnumber = -1
			rownum+=1
			for cellvalue in row:
				colnumber += 1
				worksheet.write(rownum,colnumber,cellvalue)

		workbook.close()
		return True

	def get_last_checked_placement_id(self):
		try:
			f=open("lastplacementid.txt", "r")
			self.last_checked_placement_id =int(f.read())
			f.close()
		except:
			self.last_checked_placement_id = 0

	def get_last_checked_creative_id(self):
		try:
			f=open("lastcreativeid.txt", "r")
			self.creative_qa_history = json.load(f)
			f.close()		
		# try:
		# 	f=open("lastcreativeid.txt", "r")
		# 	self.last_checked_creative_id =int(f.read())
		# 	self.creative_qa_history = json.load(f)
		# 	f.close()		
		# 	self.qa_results(self.creative_qa_history)
		# 	self.qa_results(self.last_checked_creative_id)
		except:
		# 	self.last_checked_creative_id = 0
			self.creative_qa_history = {}
			for advertiserId in self.dss_advertisers:
				self.creative_qa_history[str(advertiserId)]=0

	def extract_creative_taxonomy(self):
		df = pandas.read_excel(self.file_name, "cr_data", header=1,keep_default_na=False, na_values=[''])
		self.creative_taxonomy = {}
		for item in df.columns:
			self.creative_taxonomy[item] = df[df[item].notnull()][item].unique().tolist()

	def extract_taxonomy(self)			:
		df = pandas.read_excel(self.file_name, "data",keep_default_na=False, na_values=[''])
		prog_index = df.columns.get_loc("PROG & SD")
		social_index = df.columns.get_loc("Social")
		direct_index = df.columns.get_loc("site direct & owned and operated")
		search_index = df.columns.get_loc("Search")
		inapp_index = df.columns.get_loc("in-app")
		ott_index = df.columns.get_loc("OTT")
		performance_index = df.columns.get_loc("PERFORMANCE \nPARTNERSHIPS")

		prog_df = df.iloc[:,prog_index:social_index-1]
		prog_df = self.remove_dataframe_header(prog_df)
		for item in prog_df.columns:
			self.prog_taxonomy[item] = prog_df[prog_df[item].notnull()][item].unique().tolist()

		social_df = df.iloc[:,social_index+1:direct_index-1]
		social_df = self.remove_dataframe_header(social_df)
		for item in social_df.columns:
			self.social_taxonomy[item] = social_df[social_df[item].notnull()][item].unique().tolist()		

		direct_df = df.iloc[:,direct_index+1:search_index-1]
		direct_df = self.remove_dataframe_header(direct_df)
		for item in direct_df.columns:
			self.direct_taxonomy[item] = direct_df[direct_df[item].notnull()][item].unique().tolist()				

		search_df = df.iloc[:,search_index+1:inapp_index-1]
		search_df = self.remove_dataframe_header(search_df)
		for item in search_df.columns:
			self.search_taxonomy[item] = search_df[search_df[item].notnull()][item].unique().tolist()	

		inapp_df = df.iloc[:,inapp_index+1:ott_index-1]
		inapp_df = self.remove_dataframe_header(inapp_df)
		for item in inapp_df.columns:
			self.inapp_taxonomy[item] = inapp_df[inapp_df[item].notnull()][item].unique().tolist()		

		ott_df = df.iloc[:,ott_index+1:performance_index-1]
		ott_df = self.remove_dataframe_header(ott_df)
		for item in ott_df.columns:
			self.ott_taxonomy[item] = ott_df[ott_df[item].notnull()][item].unique().tolist()				

		performance_df = df.iloc[:,performance_index+1:]
		performance_df = self.remove_dataframe_header(performance_df)
		for item in performance_df.columns:
			self.performance_taxonomy[item] = performance_df[performance_df[item].notnull()][item].unique().tolist()			

	def remove_dataframe_header(self, df):
		df.to_csv('temp.csv', header=False, index=False)
		df = pandas.read_csv('temp.csv',keep_default_na=False)		
		return df

	def get_placement_taxonomy_file(self):
		DRIVE = discovery.build('drive', 'v3', http=self.creds.authorize(Http()))

		FILENAME = 'placement taxonomy gdrive'
		SRC_MIMETYPE = 'application/vnd.google-apps.spreadsheet'
		DST_MIMETYPE = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

		files = DRIVE.files().list(
		    q='name="%s" and mimeType="%s"' % (FILENAME, SRC_MIMETYPE),
		    orderBy='modifiedTime desc,name').execute().get('files', [])

		if files:
		    self.file_name = '%s.xlsx' % os.path.splitext(files[0]['name'].replace(' ', '_'))[0]
		    # self.qa_results("Downloading latest taxonomy sheet... ")
		    data = DRIVE.files().export(fileId=files[0]['id'], mimeType=DST_MIMETYPE).execute()
		    if data:
		        with open(self.file_name, 'wb') as f:
		            f.write(data)
		        # self.qa_results('DONE')
		        return True
		    else:
		        self.qa_results('ERROR (could not download tax file)')
		else:
		    self.qa_results('!!! ERROR: tax File not found')    
		    return False

	def print_tax_errors(self, errors):
		for error in errors:
			try:
				for key, value in error.items():
					self.qa_results("\"" + str(value) + "\" is not a valid " + str(key))
			except:
				pass

	def check_creative_vs_tax(self, input_string):
		errors=[]
		values_array = input_string.split('_')
		if len(values_array) < 16:
			return {"results":False, "errors":["wrong amount of delimiters"]}
		if values_array[0] == "":
			errors.append({"ID - Internal":values_array[0]})
		if values_array[1] not in self.creative_taxonomy['Campaign Owner']:
			errors.append({"Campaign Owner":values_array[1]})
		if values_array[2] not in self.creative_taxonomy['Product']:
			errors.append({"Product":values_array[2]})
		if values_array[3] not in self.creative_taxonomy['Content Types']:
			errors.append({"Content Types":values_array[3]})
		if values_array[4] not in self.creative_taxonomy['Content Category']:
			errors.append({"Content Category":values_array[4]})
		if values_array[5] not in self.creative_taxonomy['Campaign']:
			errors.append({"Campaign":values_array[5]})
		if values_array[6] =="":
			errors.append({"Title":values_array[6]})
		if values_array[7] not in self.creative_taxonomy['Type']:
			errors.append({"Type":values_array[7]})
		if values_array[8] not in self.creative_taxonomy['Logo vs Image']:
			errors.append({"Logo vs Image":values_array[8]})
		if values_array[9] not in self.creative_taxonomy['Offer type']:
			errors.append({"Offer type":values_array[9]})
		if values_array[10] not in self.creative_taxonomy['CTA']:
			errors.append({"CTA":values_array[10]})
		if values_array[11] not in self.creative_taxonomy['Execution Type']:
			errors.append({"Execution Type":values_array[11]})
		if values_array[12] not in self.creative_taxonomy['SpecSize']:
			errors.append({"Spec Size":values_array[12]})
		if values_array[13] not in self.creative_taxonomy['Video Length']:
			if values_array[13]!="NA":
				errors.append({"Video Length":values_array[13]})
		if values_array[14] not in self.creative_taxonomy['DCO']:
			errors.append({"DCO":values_array[14]})
		if values_array[15] not in self.creative_taxonomy['Language']:
			errors.append({"Language":values_array[15]})
		if len(errors) > 0:
			return {"results":False, "errors":errors}
		else:
			return {"results":True}	

	def check_search_placement_vs_tax(self, input_string):
		errors=[]
		values_array = input_string.split('|')
		if len(values_array)==1:
			values_array = input_string.split('_')
		if len(values_array)<7:
			return {"results":False, "errors":["wrong amount of delimiters"], "channel":"search"}
		if values_array[0] == "":
			errors.append({"S360 campaign name":values_array[0]})
		if values_array[1] not in self.search_taxonomy['Landing Geo']:
			errors.append({"Landing Geo":values_array[1]})
		if values_array[2] not in self.search_taxonomy['Unnamed: 29']:
			errors.append({"Campaign Objective":values_array[2]})
		if values_array[3] not in self.search_taxonomy['Channel ']:
			errors.append({"Channel ":values_array[3]})
		if values_array[4] not in self.search_taxonomy['Vendor Name']:
			errors.append({"Vendor Name":values_array[4]})
		if values_array[5] not in self.search_taxonomy['search ad format abbr']:
			errors.append({"Ad Format":values_array[5]})
		if values_array[6] not in self.search_taxonomy['audience2 - ABBREVIATED NAME']:
			errors.append({"Audience 2":values_array[6]})
		if len(errors) > 0:
			return {"results":False, "errors":errors, "channel":"search"}
		else:
			return {"results":True}	

	def check_prog_placement_vs_tax(self, input_string):
		errors=[]
		values_array = input_string.split('|')
		if len(values_array)==1:
			values_array = input_string.split('_')
		if len(values_array)<26:
			return {"results":False, "errors":["wrong amount of delimiters"], "channel":"programmatic"}
		if values_array[0] not in self.prog_taxonomy['Campaign Owner']:
			errors.append({"Campaign Owner":values_array[0]})
		if values_array[1] not in self.prog_taxonomy['Business Unit']:
			errors.append({"Product":values_array[1]})
		if values_array[2] not in self.prog_taxonomy['Buying Geo']:
			errors.append({"Buying Geo":values_array[2]})
		if values_array[3] not in self.prog_taxonomy['Landing Geo']:
			errors.append({"Landing Geo":values_array[3]})
		if values_array[4] not in self.prog_taxonomy['Campaign Focus']:
			errors.append({"Campaign Focus":values_array[4]})
		if values_array[5] not in self.search_taxonomy['Unnamed: 29']:
			errors.append({"Campaign Objective":values_array[5]})
		if values_array[6] not in self.prog_taxonomy['Channel ']:
			errors.append({"Channel ":values_array[6]})
		if values_array[7] not in self.prog_taxonomy['Device']:
			errors.append({"Device":values_array[7]})
		if values_array[8] not in self.prog_taxonomy['Vendor Name']:
			errors.append({"Vendor Name":values_array[8]})
		if values_array[9] not in self.prog_taxonomy['Buy (Funding) Type']:
			errors.append({"Buy (Funding) Type":values_array[9]})
		if values_array[10] not in self.prog_taxonomy['Ad Format']:
			errors.append({"Ad Format":values_array[10]})
		if values_array[11] not in self.prog_taxonomy['Ad Type']:
			errors.append({"Ad Type":values_array[11]})
		if values_array[12] not in self.prog_taxonomy['Unit Size']:
			errors.append({"Unit Size":values_array[12]})
		if values_array[13] not in self.prog_taxonomy['Programmatic Exchange']:
			errors.append({"Programmatic Exchange":values_array[13]})
		if values_array[14] not in self.prog_taxonomy[' Exchange Site']:
			errors.append({"Exchange Site":values_array[14]})
		if values_array[15] not in self.prog_taxonomy['Targeting Strategy']:
			errors.append({"Targeting Strategy":values_array[15]})
		if values_array[16] not in self.prog_taxonomy['Targeting Tactic 1']:
			errors.append({"Targeting Tactic 1":values_array[16]})
		if values_array[17] not in self.prog_taxonomy['Targeting Tactic 2']:
			errors.append({"Targeting Tactic 2":values_array[17]})
		if values_array[18] not in self.prog_taxonomy['Recency']:
			errors.append({"Recency":values_array[18]})
		if values_array[19] not in self.prog_taxonomy['Data Type/Source']:
			errors.append({"Data Type/Source":values_array[19]})
		if values_array[20] not in self.prog_taxonomy['Audience Type']:
			errors.append({"Audience Type":values_array[20]})
		if values_array[21] not in self.search_taxonomy['audience2 - ABBREVIATED NAME']:
			errors.append({"Audience 2":values_array[21]})
		if values_array[22] not in self.prog_taxonomy['Ad Server']:
			errors.append({"Ad Server":values_array[22]})
		if values_array[24] not in self.prog_taxonomy['League']:
			errors.append({"League":values_array[24]})
		if values_array[25] not in self.prog_taxonomy['Audience 3']:
			errors.append({"Audience 3":values_array[25]})
		if len(errors) > 0:
			return {"results":False, "errors":errors, "channel":"programmatic display"}
		else:
			return {"results":True}

	def check_direct_placement_vs_tax(self, input_string):
		errors=[]
		values_array = input_string.split('|')
		if len(values_array)<23:
			return {"results":False, "errors":["wrong amount of delimiters"], "channel":"direct"}		
		if len(values_array)==1:
			values_array = input_string.split('_')
		if values_array[0] not in self.direct_taxonomy['Campaign Owner']:
			errors.append({"Campaign Owner":values_array[0]})
		if values_array[1] not in self.direct_taxonomy['Business Unit']:
			errors.append({"Product":values_array[1]})
		if values_array[2] not in self.direct_taxonomy['Buying Geo']:
			errors.append({"Buying Geo":values_array[2]})
		if values_array[3] not in self.direct_taxonomy['Landing Geo']:
			errors.append({"Landing Geo":values_array[3]})
		if values_array[4] not in self.direct_taxonomy['Campaign Focus']:
			errors.append({"Campaign Focus":values_array[4]})
		if values_array[5] not in self.search_taxonomy['Unnamed: 29']:
			errors.append({"Campaign Objective":values_array[5]})
		if values_array[6] not in self.direct_taxonomy['Channel ']:
			errors.append({"Channel ":values_array[6]})
		if values_array[7] not in self.direct_taxonomy['Device']:
			errors.append({"Device":values_array[7]})
		if values_array[8] not in self.direct_taxonomy['Vendor Name']:
			errors.append({"Vendor Name":values_array[8]})
		if values_array[9] not in self.direct_taxonomy['Buy (Funding) Type']:
			errors.append({"Buy (Funding) Type":values_array[9]})
		if values_array[10] not in self.direct_taxonomy['Ad Format']:
			errors.append({"Ad Format":values_array[10]})
		if values_array[11] not in self.direct_taxonomy['Ad Type']:
			errors.append({"Ad Type":values_array[11]})
		if values_array[12] not in self.direct_taxonomy['Unit Size']:
			errors.append({"Unit Size":values_array[12]})
		if values_array[13] not in self.direct_taxonomy['Programmatic Exchange']:
			errors.append({"Programmatic Exchange":values_array[13]})
		if values_array[14] not in self.direct_taxonomy[' Exchange Site']:
			errors.append({"Exchange Site":values_array[14]})
		if values_array[15] not in self.direct_taxonomy['Targeting Strategy']:
			errors.append({"Targeting Strategy":values_array[15]})
		if values_array[16] not in self.direct_taxonomy['Targeting Tactic 1']:
			errors.append({"Targeting Tactic 1":values_array[16]})
		if values_array[17] not in self.direct_taxonomy['Targeting Tactic 2']:
			errors.append({"Targeting Tactic 2":values_array[17]})
		if values_array[18] not in self.direct_taxonomy['Recency']:
			errors.append({"Recency":values_array[18]})
		if values_array[19] not in self.direct_taxonomy['Data Type/Source']:
			errors.append({"Data Type/Source":values_array[19]})
		if values_array[20] not in self.direct_taxonomy['Audience Type']:
			errors.append({"Audience Type":values_array[20]})
		if values_array[21] not in self.search_taxonomy['audience2 - ABBREVIATED NAME']:
			errors.append({"Audience 2":values_array[21]})
		if values_array[22] not in self.direct_taxonomy['Ad Server']:
			errors.append({"Ad Server":values_array[22]})

		if len(errors) > 0:
			return {"results":False, "errors":errors, "channel":"direct"}
		else:
			return {"results":True}

	def check_social_placement_vs_tax(self, input_string):
		values_array = input_string.split('|')
		errors=[]
		if len(values_array)==1:
			values_array = input_string.split('_')
		if len(values_array)<26:
			return {"results":False, "errors":["wrong amount of delimiters"], "channel":"social"}			
		if values_array[0] not in self.social_taxonomy['Campaign Owner']:
			errors.append({"Campaign Owner":values_array[0]})
		if values_array[1] not in self.social_taxonomy['Business Unit']:
			errors.append({"Product":values_array[1]})
		if values_array[2] not in self.social_taxonomy['Buying Geo']:
			errors.append({"Buying Geo":values_array[2]})
		if values_array[3] not in self.social_taxonomy['Landing Geo']:
			errors.append({"Landing Geo":values_array[3]})
		if values_array[4] not in self.social_taxonomy['Campaign Focus']:
			errors.append({"Campaign Focus":values_array[4]})
		if values_array[5] not in self.search_taxonomy['Unnamed: 29']:
			errors.append({"Campaign Objective":values_array[5]})
		if values_array[6] not in self.social_taxonomy['Channel ']:
			errors.append({"Channel ":values_array[6]})
		if values_array[7] not in self.social_taxonomy['Device']:
			errors.append({"Device":values_array[7]})
		if values_array[8] not in self.social_taxonomy['Vendor Name']:
			errors.append({"Vendor Name":values_array[8]})
		if values_array[9] not in self.social_taxonomy['Buy (Funding) Type']:
			errors.append({"Buy (Funding) Type":values_array[9]})
		if values_array[10] not in self.social_taxonomy['Ad Format']:
			errors.append({"Ad Format":values_array[10]})
		if values_array[11] not in self.social_taxonomy['Ad Type']:
			errors.append({"Ad Type":values_array[11]})
		if values_array[12] not in self.social_taxonomy['Unit Size']:
			errors.append({"Unit Size":values_array[12]})
		if values_array[13] not in self.social_taxonomy['Social Ad Placement']:
			errors.append({"Social Ad Placement":values_array[13]})
		if values_array[14] == "":
			errors.append({"Exchange Site":values_array[14]})
		if values_array[15] not in self.social_taxonomy['Targeting Strategy']:
			errors.append({"Targeting Strategy":values_array[15]})
		if values_array[16] not in self.social_taxonomy['Targeting Tactic 1']:
			errors.append({"Targeting Tactic 1":values_array[16]})
		if values_array[17] not in self.social_taxonomy['Targeting Tactic 2']:
			errors.append({"Targeting Tactic 2":values_array[17]})
		if values_array[18] not in self.social_taxonomy['Recency']:
			errors.append({"Recency":values_array[18]})
		if values_array[19] not in self.social_taxonomy['Data Type/Source']:
			errors.append({"Data Type/Source":values_array[19]})
		if values_array[20] not in self.social_taxonomy['Audience Type']:
			errors.append({"Audience Type":values_array[20]})
		if values_array[21] not in self.search_taxonomy['audience2 - ABBREVIATED NAME']:
			errors.append({"Audience 2":values_array[21]})
		if values_array[22] not in self.social_taxonomy['Ad Server']:
			errors.append({"AdServer":values_array[22]})
		if values_array[24] not in self.social_taxonomy['League']:
			errors.append({"League":values_array[24]})
		if values_array[25] not in self.social_taxonomy['Audience 3']:
			errors.append({"Audience 3":values_array[25]})

		if len(errors) > 0:
			return {"results":False, "errors":errors, "channel":"social"}
		else:
			return {"results":True}

	def check_performance_placement_vs_tax(self, input_string):
		errors=[]
		values_array = input_string.split('|')
		if len(values_array)==1:
			values_array = input_string.split('_')
		if len(values_array)<11:
			return {"results":False, "errors":["wrong amount of delimiters"], "channel":"performance"}			
		if values_array[0] not in self.performance_taxonomy['Campaign Owner']:
			errors.append({"Campaign Owner":values_array[0]})
		if values_array[1] not in self.performance_taxonomy['Product']:
			errors.append({"Product":values_array[1]})
		if values_array[2] not in self.performance_taxonomy['Network']:
			errors.append({"Network":values_array[2]})
		if values_array[3] not in self.performance_taxonomy['Landing Geo']:
			errors.append({"Landing Geo":values_array[3]})
		if values_array[4] not in self.performance_taxonomy['Campaign Focus']:
			errors.append({"Campaign Focus":values_array[4]})
		if values_array[5] not in self.performance_taxonomy['Campaign Objective']:
			errors.append({"Campaign Objective":values_array[5]})
		if values_array[6] not in self.performance_taxonomy['Channel ']:
			errors.append({"Channel ":values_array[6]})
		if values_array[7] not in self.performance_taxonomy['Ad Format']:
			errors.append({"Ad Format":values_array[7]})
		if values_array[8] not in self.performance_taxonomy['Unit Size']:
			errors.append({"Unit Size":values_array[8]})
		if values_array[9] not in self.performance_taxonomy['Audience']:
			errors.append({"Audience":values_array[9]})
		if values_array[10] == "":
			errors.append({"Audience Detail/Free Form Field":values_array[10]})
		if len(errors) > 0:
			return {"results":False, "errors":errors, "channel":"performance"}
		else:
			return {"results":True}

	def check_inapp_placement_vs_tax(self, input_string):
		errors=[]
		values_array = input_string.split('|')
		if len(values_array)==1:
			values_array = input_string.split('_')
		if len(values_array)<13:
			return {"results":False, "errors":["wrong amount of delimiters"], "channel":"In-App"}			
		if values_array[0] not in self.inapp_taxonomy['Campaign Owner']:
			errors.append({"Campaign Owner":values_array[0]})
		if values_array[1] not in self.inapp_taxonomy['Product']:
			errors.append({"Product":values_array[1]})
		if values_array[2] not in self.inapp_taxonomy['Campaign Focus']:
			errors.append({"Campaign Focus":values_array[2]})
		if values_array[3] not in self.search_taxonomy['Unnamed: 29']:
			errors.append({"Campaign Objective":values_array[3]})
		if values_array[4] == "":
			errors.append({"Campaign Name":values_array[4]})
		if values_array[5] not in self.inapp_taxonomy['Landing Geo']:
			errors.append({"Landing Geo":values_array[5]})
		if values_array[6] not in self.inapp_taxonomy['App']:
			errors.append({"App":values_array[6]})
		if values_array[7] not in self.inapp_taxonomy['OS']:
			errors.append({"OS":values_array[7]})
		if values_array[8] == "":
			errors.append({"Audience Name":values_array[8]})
		if values_array[9] not in self.inapp_taxonomy['Trigger']:
			errors.append({"Trigger":values_array[9]})
		if values_array[10] == "":
			errors.append({"Trigger Attribute":values_array[10]})
		if values_array[11] not in self.inapp_taxonomy['Creative Format']:
			errors.append({"Creative Format":values_array[11]})
		if values_array[12] != "In-App":
			errors.append({"Channel":values_array[12]})
		if len(errors) > 0:
			return {"results":False, "errors":errors, "channel":"In-App"}
		else:
			return {"results":True}

	def check_ott_placement_vs_tax(self, input_string):
		errors=[]
		values_array = input_string.split('|')
		if len(values_array)==1:
			values_array = input_string.split('_')
		if len(values_array)<23:
			return {"results":False, "errors":["wrong amount of delimiters"], "channel":"OTT"}			
		if values_array[0] not in self.ott_taxonomy['Campaign Owner']:
			errors.append({"Campaign Owner":values_array[0]})
		if values_array[1] not in self.ott_taxonomy['Product']:
			errors.append({"Product":values_array[1]})
		if values_array[2] not in self.ott_taxonomy['Buying Geo']:
			errors.append({"Buying Geo":values_array[2]})
		if values_array[3] not in self.ott_taxonomy['Landing Geo']:
			errors.append({"Landing Geo":values_array[3]})
		if values_array[4] not in self.ott_taxonomy['Campaign Focus']:
			errors.append({"Campaign Focus":values_array[4]})
		if values_array[5] not in self.search_taxonomy['Unnamed: 29']:
			errors.append({"Campaign Objective":values_array[5]})
		if values_array[6] not in self.ott_taxonomy['Channel ']:
			errors.append({"Channel ":values_array[6]})
		if values_array[7] not in self.ott_taxonomy['Device']:
			errors.append({"Device":values_array[7]})
		if values_array[8] not in self.ott_taxonomy['Vendor Name']:
			errors.append({"Vendor Name":values_array[8]})
		if values_array[9] not in self.ott_taxonomy['Buy (Funding) Type']:
			errors.append({"Buy (Funding) Type":values_array[9]})
		if values_array[10] not in self.ott_taxonomy['Ad Format']:
			errors.append({"Ad Format":values_array[10]})
		if values_array[11] not in self.ott_taxonomy['Ad Type']:
			errors.append({"Ad Type":values_array[11]})
		if values_array[12] not in self.ott_taxonomy['Unit Size']:
			errors.append({"Unit Size":values_array[12]})
		if values_array[13] not in self.ott_taxonomy['Programmatic Exchange']:
			errors.append({"Programmatic Exchange":values_array[13]})
		if values_array[14] not in self.ott_taxonomy[' Exchange Site']:
			errors.append({"Exchange Site":values_array[14]})
		if values_array[15] not in self.ott_taxonomy['Targeting Strategy']:
			errors.append({"Targeting Strategy":values_array[15]})
		if values_array[16] not in self.ott_taxonomy['Targeting Tactic 1']:
			errors.append({"Targeting Tactic 1":values_array[16]})
		if values_array[17] not in self.ott_taxonomy['Targeting Tactic 2']:
			errors.append({"Targeting Tactic 2":values_array[17]})
		if values_array[18] not in self.ott_taxonomy['Recency']:
			errors.append({"Recency":values_array[18]})
		if values_array[19] not in self.ott_taxonomy['Data Type/Source']:
			errors.append({"Data Type/Source":values_array[19]})
		if values_array[20] not in self.ott_taxonomy['Audience Type']:
			errors.append({"Audience Type":values_array[20]})
		if values_array[21] not in self.search_taxonomy['audience2 - ABBREVIATED NAME']:
			errors.append({"Audience 2":values_array[21]})
		if values_array[22] not in self.ott_taxonomy['Ad Server']:
			errors.append({"Ad Server":values_array[22]})
		if len(errors) > 0:
			return {"results":False, "errors":errors, "channel":"OTT"}
		else:
			return {"results":True}

	def determine_channel(self, input_string):
		try:
			# self.qa_results('determining channel from '+input_string)
			values_array = input_string.split('|')
			if len(values_array)==1:
				values_array = input_string.split('_')
			if values_array[6]=="Social":
				return "Social"
			if values_array[3]=="Search":
				return "Search"
			if values_array[6]=="OTT":
				return "OTT"			
			if values_array[6] in self.prog_taxonomy['Channel ']:
				return "Programmatic"
			if values_array[6] in self.direct_taxonomy['Channel ']:
				return "Direct"
			# if values_array[11]=="Email":
			# 	return "Email"	
			if values_array[6]=="PerformancePartnerships":
				return "Perf"
			if values_array[12] == "In-App":
				return "InApp"
			self.qa_results("Could not determine channel for "+input_string)
			return False
		except:
			self.qa_results("Could not determine channel for "+str(input_string))
			return False			

	def qa_placements(self):
		try:
		# Construct the request.
			if debug:
				maxResults = 3
			else:
				maxResults = 1000
			request = self.service.placements().list(profileId=self.user_profile, advertiserIds=self.dss_advertisers, archived=False, sortField="ID",sortOrder="DESCENDING", maxResults = maxResults)

			while True:
				# Execute request and print response.
				response = request.execute()

				for placement in response['placements']:
					if int(placement['id']) <= self.last_checked_placement_id:
						self.qa_results( "No more placements to check")
						if self.new_last_placement_id != 0:
							f= open("lastplacementid.txt","w+")
							f.write(str(self.new_last_placement_id))
							f.close()
						return True
					else:
						qa_results = self.qa_placement(placement)
						if qa_results['results'] == False:
							self.qa_results('Found placement with ID %s and name "%s" violates taxonomy.' % (placement['id'], placement['name']))
							self.bad_placements.append({"id":placement['id'],"channel":qa_results['channel'],"name":placement['name'],"errors":qa_results['errors']})
						if int(placement['id']) > self.new_last_placement_id:
							self.new_last_placement_id = int(placement['id'])						

				if response['placements'] and response['nextPageToken']:
					if debug:
						break
					request = self.service.placements().list_next(request, response)
				else:
					break

		except client.AccessTokenRefreshError:
			self.qa_results('The credentials have been revoked or expired, please re-run the application to re-authorize')

		if self.last_checked_placement_id == 0:
			f= open("lastplacementid.txt","w+")
			f.write(str(self.new_last_placement_id))
			f.close()			

	def qa_creatives(self):
		for advertiserId in self.dss_advertisers:
			self.qa_creatives_for_advertiser(advertiserId)
			self.qa_results( "No more creatives to check for advertiserId "+str(advertiserId))
			if self.new_last_creative_id != 0:
				self.creative_qa_history[str(advertiserId)] = self.new_last_creative_id
		f= open("lastcreativeid.txt","w+")
		json.dump(self.creative_qa_history,f)
		f.close()

	def creatives_all_checked_for_adveriser(self, creativeId, advertiserId):
		# self.qa_results("checkinf if "+str(creativeId)+" is less than or equal to "+str(self.creative_qa_history[str(advertiserId)]))
		return  int(creativeId) <= self.creative_qa_history[str(advertiserId)]

	def qa_creatives_for_advertiser(self, advertiserId):
		self.new_last_creative_id = 0
		try:
			if debug:
				maxResults = 3
			else:
				maxResults = 1000
			request = self.service.creatives().list(profileId=self.user_profile, advertiserId=advertiserId, archived=False, sortField="ID",sortOrder="DESCENDING", maxResults = maxResults)

			while True:
				response = request.execute()

				for creative in response['creatives']:
					if self.creatives_all_checked_for_adveriser( int(creative['id']), advertiserId):
						return True
					else:
						qa_results = self.check_creative_vs_tax(creative['name'])
						if qa_results['results'] == False:
							# self.qa_results('Found creative with ID %s and name "%s" violates taxonomy.' % (creative['id'], creative['name']))
							self.bad_creatives.append({"id":creative['id'],"name":creative['name'],"errors":qa_results['errors']})
						if int(creative['id']) > self.new_last_creative_id:
							self.new_last_creative_id = int(creative['id'])						

				if response['creatives'] and response['nextPageToken']:
					if debug:
						break
					request = self.service.creatives().list_next(request, response)
				else:
					break

		except client.AccessTokenRefreshError:
			self.qa_results('The credentials have been revoked or expired, please re-run the application to re-authorize')
	
	def qa_placement(self, placement):
		channel = self.determine_channel(placement['name'])
		if channel=="Social":
			return self.check_social_placement_vs_tax(placement['name'])
		if channel=="Search":
			return self.check_search_placement_vs_tax(placement['name'])
		if channel=="OTT":
			return self.check_ott_placement_vs_tax(placement['name'])
		if channel=="Programmatic":
			return self.check_prog_placement_vs_tax(placement['name'])
		if channel== "Direct":
			return self.check_direct_placement_vs_tax(placement['name'])
		if channel== "Perf":
			return self.check_performance_placement_vs_tax(placement['name'])
		if channel == "In-App":
			return self.check_inapp_placement_vs_tax(placement['name'])
		return {"results":False, "errors":["Unknown Channel"], "channel":"Unknown Channel"}

	def global_qa(self):
		self.qa_placements()
		self.qa_creatives()
		self.generate_results_spreadsheet()

	def output_failure_message(self, object, errors):
		self.qa_results()
		self.qa_results('"%s" violates taxonomy.' % (object))
		self.print_tax_errors(errors)

	def qa_traffic_sheet(self, input_sheet):
		if debug:
			self.qa_results("Beginning Traffic Sheet QA")
		reject = False
		df = pandas.read_excel(input_sheet)
		if 'Ad Set Name' in df.columns:
			for placement in df['Ad Set Name']:
				qa_results = self.check_social_placement_vs_tax(placement)
				if qa_results['results'] == False:
					self.output_failure_message(placement,qa_results['errors'])
					self.bad_placements.append({"name":placement,"errors":qa_results['errors']})
					reject = True
		elif  df.iloc[7,1] == "Placement ID":	
			df = df.iloc[7:]
			new_header = df.iloc[0] 
			df = df[1:] 
			df.columns = new_header 
			creative_column_names = []
			url_column_names = []
			for column_name in df.columns:
				if column_name[:-2] == "Creative  Name" or column_name[:-2] == "Creative Name":
					creative_column_names.append(column_name)
				if column_name.find("URL")>-1:
					url_column_names.append(column_name)			
			placements = df['Placement Name']
			for placement in placements:
				if type(placement) == float:
					if debug:
						self.qa_results()
						self.qa_results("Blank placement name skipped")				
				else:
					qa_results = self.qa_placement({'name':placement})
					for url_column_name in url_column_names:
						url = df.loc[df['Placement Name'] == placement][url_column_name].values[0]
						if type(url) != float:
							if not self.qa_clickthrough_url(url, placement):
								qa_results['results'] = False
								if "errors" in qa_results:
									qa_results['errors'].append({url_column_name:url})
								else:
									qa_results['errors'] = [{url_column_name:url}]
									reject = True
					if qa_results['results'] == False:
						self.output_failure_message(placement,qa_results['errors'])
						self.bad_placements.append({"name":placement,"errors":qa_results['errors']})
						reject = True
			for creative_column in creative_column_names:
				for creative in df[creative_column]:
					if type(creative) == float:
						pass
						# if debug:
							# self.qa_results()
							# self.qa_results("Blank creative name skipped")
					else:
						qa_results = self.check_creative_vs_tax(creative)
						if qa_results['results'] == False:
							self.output_failure_message(creative,qa_results['errors'])
							self.bad_creatives.append({"name":creative,"errors":qa_results['errors']})
							reject = True
		else:
			self.qa_results("Could not determine type of traffic sheet")
			reject = True

		if reject:
			self.qa_results()
			self.qa_results("Traffic sheet rejected")
		else:
			self.qa_results()
			self.qa_results("No problems found! Good to go.")

	def populate_master_errors_list(self):
		for placement in self.bad_placements:
			for error in placement['errors']:
				try:
					for error_type, error_value in error.items():
						if error_value not in self.master_errors_list['placement_errors'][error_type]:
							self.master_errors_list['placement_errors'][error_type].append(error_value)
				except:
					if error not in self.master_errors_list['placement_errors']['generic_error']:
						self.master_errors_list['placement_errors']['generic_error'].append(error)

		for creative in self.bad_creatives:
			for error in creative['errors']:
				try:
					for error_type, error_value in error.items():
						if error_value not in self.master_errors_list['creative_errors'][error_type]:
							self.master_errors_list['creative_errors'][error_type].append(error_value)					
				except:
					if error not in self.master_errors_list['creative_errors']['generic_error']:
						self.master_errors_list['creative_errors']['generic_error'].append(error)						

	def qa_results(self, message="<br>"):
		print(message)
		self.logstring+="<br>"+message

	def export_master_errors(self):
		workbook = xlsxwriter.Workbook(str(self.tempfolder)+"/taxonomy_errors.xlsx")
		worksheet = workbook.add_worksheet('placement_errors')

		colnumber = -1
		for error_type in self.master_errors_list['placement_errors']:
			rownum = -1
			colnumber += 1
			for error_value in self.master_errors_list['placement_errors'][error_type]:
				if rownum == -1:
					rownum+=1
					worksheet.write(rownum,colnumber,error_type)
				rownum+=1
				worksheet.write(rownum,colnumber,error_value)

		colnumber = -1
		worksheet = workbook.add_worksheet('creative_errors')
		for error_type in self.master_errors_list['creative_errors']:
			rownum = -1
			colnumber += 1
			for error_value in self.master_errors_list['creative_errors'][error_type]:
				if rownum == -1:
					rownum+=1
					worksheet.write(rownum,colnumber,error_type)				
				rownum+=1
				worksheet.write(rownum,colnumber,error_value)

		workbook.close()

	def readlog(self):
		with open (str(self.tempfolder)+"/results.txt", "r") as myfile:
			return myfile.read().replace('\n', '<br>')
		
	def qa_clickthrough_url(self, clickthrough_url, placement_name):
		qsp = clickthrough_url[clickthrough_url.find("?ex_cid=")+8:]
		owner = placement_name[:placement_name.find("|")]
		remainder = placement_name.replace(re.search("^(.*?\|)(.*?\|)(.*?\|)(.*?\|)(.*?\|)(.*?\|)",placement_name).group(),"")
		channel = remainder[:remainder.find("|")]
		remainder = remainder.replace(re.search("^(.*?\|)(.*?\|)",remainder).group(),"")
		vendor_platform = remainder[:remainder.find("|")]
		return qsp == owner+"-"+channel+"-"+vendor_platform+"-%ebuy!-%epid!-%eaid!-%ecid!"

if __name__ == '__main__':
	debug = False
	qa = taxonomy_qa()
	if len(sys.argv) == 1:
		qa.global_qa()
	else:
		qa.qa_traffic_sheet(sys.argv[1])

	qa.populate_master_errors_list()
	qa.export_master_errors()
	# qa.writer.close()		
	# input("Press enter to exit")


else:
	debug = True
