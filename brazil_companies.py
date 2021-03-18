import apache_beam as beam
from datetime import datetime
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import argparse

from apache_beam.coders.coders import Coder
from google.cloud import bigquery

import sys


#the public data is in iso-8859-1
#source: https://medium.com/@khushboo_16578/cloud-dataflow-and-iso-8859-1-2bb8763cc7c8
class ISOCoder(Coder):
    """A coder used for reading and writing strings as ISO-8859-1."""

    def encode(self, value):
        return value.encode('iso-8859-1')

    def decode(self, value):
        return value.decode('iso-8859-1')

    def is_deterministic(self):
        return True

parser = argparse.ArgumentParser()

parser.add_argument('--input',
                   dest='input',
                   required=True,
                   help='Input file to process'
                   )

parser.add_argument('--output',
                   dest='output',
                   required=True,
                   help='Output file to write'
                   )

# parser.add_argument('--dataset',
#                    dest='dataset',
#                    required=True,
#                    help='Big query dataset'
#                    )

path_args, pipeline_args = parser.parse_known_args()

inputs_pattern = path_args.input
outputs_prefix = path_args.output
project_id = pipeline_args[pipeline_args.index('--project')+1]
# dataset_bq = path_args.dataset

options = PipelineOptions(pipeline_args)

def check_date(str_date,format_in,format_out):
  if str_date == '00000000' or str_date == '        ':
    return ''
  else:
    return datetime.strptime(str_date, format_in).strftime(format_out)

def mapping_companies(element):
  flag_table = element[0]
  flag_full = element[1]
  flag_refresh = element[2]
  company_id = element[3:17] #cnpj
  flag_hq = element[17] #flag matriz
  company_name = element[18:168] #razao social
  company_desc = element [168:223] #nome fantasia
  situation_id = element[223:225] #situacao cadastral

  situation_date = check_date(element[225:233],'%Y%m%d','%Y-%m-%d')

  situation_reason = element[233:235]
  foreign_city = element[235:290]
  country_id = element[290:293]
  country = element[293:262]
  legal_id = element[363:367] #natureza juridica

  company_signup_date = check_date(element[367:375],'%Y%m%d','%Y-%m-%d') #data inicio atividade

  company_cnae = element[375:382]
  company_address_type = element[382:402] #tipo logradouro
  company_address = element[402:462]
  company_address_number = element[462:468]
  company_address_line2 = element[468:624] #complemento
  company_address_neighborhood = element[624:674] #bairro
  company_postal_code = element[674:682]
  company_state = element[682:684]
  city_id = element[684:688]
  company_city = element[688:738] #municipio
  company_phone = element[738:750] #tel 1
  company_phone2 = element[750:762] #tel 2
  company_fax = element[762:774] 
  company_email = element[774:889]
  flag_partner = element[889:891]

  company_capital = str(float(element[891:905])/100)  #capital social
  company_capital = company_capital[:company_capital.index('.')] #remove decimals 

  company_size = element[905:907]
  flag_simples_nacional = element[907:908]

  simples_nacional_signup_date = check_date(element[908:916],'%Y%m%d','%Y-%m-%d')

  simples_nacional_remove_date = check_date(element[916:924],'%Y%m%d','%Y-%m-%d')

  flag_mei = element[924:925]
  company_special_situation = element[925:948] #situacao especial

  company_special_situation_date = check_date(element[948:956],'%Y%m%d','%Y-%m-%d')
  
  results = [flag_table,flag_full,flag_refresh,company_id,flag_hq,company_name,company_desc,situation_id,situation_date,situation_reason,foreign_city,country_id,country,legal_id,company_signup_date,company_cnae,company_address_type,company_address,company_address_number,company_address_line2,company_address_neighborhood,company_postal_code,company_state,city_id,company_city,company_phone,company_phone2,company_fax,company_email,flag_partner,company_capital,company_size,flag_simples_nacional,simples_nacional_signup_date,simples_nacional_remove_date,flag_mei,company_special_situation,company_special_situation_date]

  results = list(map(lambda x:x.rstrip(),results)) #rstrip in all list elements

  return results

def format_json_company(element):
  json_string = {
    "flag_table":element[0],
    "flag_full":element[1],
    "flag_refresh":element[2],
    "company_id":element[3],
    "flag_hq":element[4],
    "company_name":element[5],
    "company_desc":element[6],
    "situation_id":element[7],
    "situation_date":element[8],
    "situation_reason":element[9],
    "foreign_city":element[10],
    "country_id":element[11],
    "country":element[12],
    "legal_id":element[13],
    "company_signup_date":element[14],
    "company_cnae":element[15],
    "company_address_type":element[16],
    "company_address":element[17],
    "company_address_number":element[18],
    "company_address_line2":element[19],
    "company_address_neighborhood":element[20],
    "company_postal_code":element[21],
    "company_state":element[22],
    "city_id":element[23],
    "company_city":element[24],
    "company_phone":element[25],
    "company_phone2":element[26],
    "company_fax":element[27],
    "company_email":element[28],
    "flag_partner":element[29],
    "company_capital":element[30],
    "company_size":element[31],
    "flag_simples_nacional":element[32],
    "simples_nacional_signup_date":element[33],
    "simples_nacional_remove_date":element[34],
    "flag_mei":element[35],
    "company_special_situation":element[36],
    "company_special_situation_date":element[37]
  }

  return json_string

def mapping_partners(element):
  flag_table = element[0]
  flag_full = element[1]
  flag_refresh = element[2]
  company_id = element[3:17] #cnpj
  partner_id = element[17]
  partner_name = element[18:168]
  partner_document = element[168:182] #cpf/cnpj
  partner_type_id = element[182:184]
  partner_percentage = element[184:189]

  partner_signup_date = check_date(element[189:197],'%Y%m%d','%Y-%m-%d')

  country_id = element[197:200]
  country = element[200:270]
  partner_legal_document = element[270:281]
  partner_legal_rep = element[281:341]
  flag_partner_legal_rep = element[341:343]

  results = [flag_table,flag_full,flag_refresh,company_id,partner_id,partner_name,partner_document,partner_type_id,partner_percentage,partner_signup_date,country_id,country,partner_legal_document,partner_legal_rep,flag_partner_legal_rep]

  results = list(map(lambda x:x.rstrip(),results)) #rstrip in all list elements

  return results

def format_json_partner(element):
  json_string = {
    "flag_table":element[0],
    "flag_full":element[1],
    "flag_refresh":element[2],
    "company_id":element[3],
    "partner_id":element[4],
    "partner_name":element[5],
    "partner_document":element[6],
    "partner_type_id":element[7],
    "partner_percentage":element[8],
    "partner_signup_date":element[9],
    "country_id":element[10],
    "country":element[11],
    "partner_legal_document":element[12],
    "partner_legal_rep":element[13],
    "flag_partner_legal_rep":element[14]
  }

  return json_string

def format_output(element):
  return ','.join(element)

with beam.Pipeline(options=options) as pipe_brazil_companies:

  extract = (
      pipe_brazil_companies
      |'Read data' >> beam.io.ReadFromText(inputs_pattern,coder=ISOCoder())
  )

  companies = (
      extract
      |'Filter company table' >> beam.Filter(lambda x: x[0]=='1')
      |'Company - mapping' >> beam.Map(mapping_companies)
      #|'Company - rstrip values' >> beam.Map(lambda x: list(map(lambda y:y.rstrip(),x))) #rstrip in all list elements
      |'Company - format' >> beam.Map(format_json_company) 
      |'Company - output data' >> beam.io.WriteToText(outputs_prefix + 'output_company')
  )

  partners = (
      extract
      |'Filter partners table' >> beam.Filter(lambda x: x[0]=='2')
      |'Partner - mapping' >> beam.Map(mapping_partners)
      #|'Partner - rstrip values' >> beam.Map(lambda x: list(map(lambda y:y.rstrip(),x))) #rstrip in all list elements
      |'Partner - format' >> beam.Map(format_json_partner)
      |'Partner - output data' >> beam.io.WriteToText(outputs_prefix + 'output_partner')
  )