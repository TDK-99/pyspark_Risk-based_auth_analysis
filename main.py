from src.config_import import builder
from src.validation import data_validation
from src.analysis import analysis,export_and_upload
from src.smtp import send_email



def main():

    print("Start building spark config and import data...",flush=True)

    spark,df = builder()

    print("Buil complete and data import, start data validation...",flush=True)

    spark,df =data_validation(spark,df)

    print("Data validation complete, start data analysis...",flush=True)

    results= analysis(spark,df)

    print("Data analysis complete, start create and upload file on s3...",flush=True)

    bytes_excel = export_and_upload(results)

    print("Data upload complete, start send me with report...",flush=True)

    send_email(bytes_excel)


main()