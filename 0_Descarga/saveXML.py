import requests
import argparse


def urldownloadMonth (url, month):

	if month < 10:
		url2 = url + '/' + str(year) + '-0' + str(month) + '.tar.gz'
	else:
		url2 = url + '/' + str(year) + '-' + str(month) + '.tar.gz'

	return (url2)

def saveUrl (path, url):
			
	response = requests.get(url, stream=True)

	if response.status_code == 200:	
		try:		
			with open(path, 'wb') as f:
				f.write(response.raw.read())
		except Exception as E:
				print ('error descargando el fichero %s' % str(E))
				return (-1)
	else:
		print ('descarga no disponible')
	return (response.status_code)
				


if __name__ == "__main__":

	parser = argparse.ArgumentParser(description='Script en python bajar datos de TED')
	parser.add_argument('-y','--year', help='Año a bajar', type=int, required=True )
	parser.add_argument('-p','--path', help='donde guardar la descarga', required=True)
	parser.add_argument('-m', '--month', help='mes para una descarga mensual', type=int, choices=range(1,13), required=False)

	arg = parser.parse_args()

	year = str(arg.year)
	url = 'https://ted.europa.eu/xml-packages/monthly-packages/' + str(year)

	if arg.month:		
		url2 = urldownloadMonth (url, arg.month)
		target_path = arg.path + '/' + url2.split('/')[-1:][0]
		saveUrl (target_path, url2)

	else:	
		for i in range (1,13):
			
			url2 = urldownloadMonth (url, i)
			print (url2)
			target_path = arg.path + '/' + url2.split('/')[-1:][0]
			print ( target_path)
			saveUrl (target_path, url2)

			


