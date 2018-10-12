from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import PDFPageAggregator
from pdfminer.layout import LAParams, LTTextBox, LTTextLine, LTFigure
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfparser import PDFParser
import pymysql 


def convert(file_name):
    lst = []
    def parser_func(layout):
        """Function to recursively parse the layout tree."""
        for lt_obj in layout:
            if isinstance(lt_obj, LTTextBox) or isinstance(lt_obj, LTTextLine):
                lst.append(lt_obj.get_text())
            elif isinstance(lt_obj, LTFigure):
                parser_func(lt_obj)  # Recursive
    text = open(file_name, 'rb')
    parser = PDFParser(text)
    doc = PDFDocument(parser)
    rsrcmgr = PDFResourceManager()
    laparams = LAParams()
    device = PDFPageAggregator(rsrcmgr, laparams=laparams)
    interpreter = PDFPageInterpreter(rsrcmgr, device)
    for page in PDFPage.create_pages(doc):
        interpreter.process_page(page)
        layout = device.get_result()
        parser_func(layout)
        if(len(lst)==22):
            lst[12:14] = [' '.join(lst[12:14])]
    return lst


def keyvalsep(lst):
    keys = []
    values = [] 

    srn = (lst[3].split('\n')[0].split(':'))
    keys.append(srn[0])
    values.append(srn[1])

    srd = lst[2].replace('\n','').split(':')
    keys.append(srd[0])
    values.append(srd[1])

    pmi = (lst[3].split('\n')[1])
    pmi = (pmi+lst[4].replace('\n','')).split(':')
    keys.append(pmi[0])
    values.append(pmi[1])

    rec_name = lst[5].split('\n')[0].replace(':','')+lst[5].split('\n')[1]
    rec_name = (rec_name+lst[6].split('\n')[0]).split(':')
    keys.append(rec_name[0])
    values.append(rec_name[1])

    rec_add = lst[5].split('\n')[0].replace(':','')+lst[5].split('\n')[2]
    rec_add = ((rec_add+' '.join(lst[6].split('\n')[1:])).split(':'))
    keys.append(rec_add[0])
    values.append(rec_add[1])

    ser_type = lst[7].split('\n')[1]
    ser_type = ser_type.split(':')
    keys.append(ser_type[0])
    values.append(ser_type[1])

    sd = lst[8].replace('\n',' :')
    sd = (sd+lst[11].replace('\n','')+lst[12].replace('\n','')).split(':')
    keys.append(sd[0])
    values.append(sd[1])

    tof = lst[9].replace('\n',':')
    tof = (tof+lst[13].replace('\n','')).split(':')
    keys.append(tof[0])
    values.append(tof[1])

    amt = lst[10].replace('\n',':')
    amt = (amt+lst[14].replace('\n','')).split(':')
    keys.append(amt[0])
    values.append(float(amt[1]))

    tot = lst[15].replace('\n',':')
    tot = (tot+lst[16].replace('\n','')).split(':')
    keys.append(tot[0])
    values.append(float(tot[1]))

    mop = lst[17].split('\n')[0]
    mop = (mop+lst[18].replace('\n','')).split(':')
    keys.append(mop[0])
    values.append(mop[1])

    rpr = (lst[17].split('\n')[1]).split(':')
    keys.append(rpr[0])
    values.append(rpr[1])

    note = (lst[19].replace('\n','')).split(':')
    keys.append(note[0])
    values.append(note[1])
    for i in range(len(values)):
        if type(values[i]) is str:
            values[i] = values[i].lstrip()
    return keys,values


file275 = convert('U16571275.pdf')

file275_k, file275_v = keyvalsep(file275)

values = []
values.append(tuple(file275_v))

# ===== Creating database, table and storing data in database ===== #

connection = pymysql.connect(host='127.0.0.1', port=5000, user='root', passwd='usr',autocommit=True)
cursor = connection.cursor()
try:
    cursor.execute('CREATE DATABASE details')
    cursor.execute('CREATE TABLE details.MINISTRY_OF_CORPORATE_AFFAIRS_RECEIPT_GAR7(SRN VARCHAR(10), Service_Request_date VARCHAR(15), Payment_Made_Into VARCHAR(30), Received_From_Name VARCHAR(50), Received_From_Address VARCHAR(150), Service_Type VARCHAR(50), Service_Description VARCHAR(100), Type_Of_Fee VARCHAR(20), Amount_Rs FLOAT(10,2), Total FLOAT(10,2), Mode_Of_Payment VARCHAR(50), Received_Payment_Rupees VARCHAR(50),Note VARCHAR(200), PRIMARY KEY(SRN))')
    rows = cursor.fetchall()
    cursor.executemany('INSERT INTO details.MINISTRY_OF_CORPORATE_AFFAIRS_RECEIPT_GAR7 VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',values)
except Exception as e:
    print("Exception occured:{}".format(e))
finally:
    cursor.close()
    connection.commit()
    connection.close()
    
# ===== Retreiving data from database ===== #
    
connection = pymysql.connect(host='127.0.0.1', port=5000, user='root', passwd='usr', db='details',autocommit=True)
cursor = connection.cursor()
try:
    cursor.execute('select * from MINISTRY_OF_CORPORATE_AFFAIRS_RECEIPT_GAR7')
    rows = cursor.fetchall()
    for row in rows:
        print(row)
except Exception as e:
    print("Exception occured:{}".format(e))
finally:
    cursor.close()
    connection.commit()
    connection.close()