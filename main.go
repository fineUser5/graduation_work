package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/biter777/countries"
	"github.com/gorilla/mux"
)

const smsDataLink string = "../skillbox-diploma/sms.data"
const mmsAddr string = "http://127.0.0.1:8383/mms"
const voiceDataLink string = "../skillbox-diploma/voice.data"
const emailDataLink string = "../skillbox-diploma/email.data"
const billingDataLink string = "../skillbox-diploma/billing.data"
const suppAddr string = "http://127.0.0.1:8383/support"
const incidentAddr string = "http://127.0.0.1:8383/accendent"

var SMSCollection [][]SMSData
var MMSCollection [][]MMSData
var VoiceCallCollection []VoiceCallData
var EmailCollection [][]EmailData //map[string][][]EmailData
var BillingCollection BillingData
var SupportCollection []int
var IncidentsCollection []IncidentData
var TestCollection ResultT

type SMSData struct {
	Сountry      string `json:"country"`
	Bandwidth    string `json:"bandwidth"`
	ResponseTime string `json:"response_time"`
	Provider     string `json:"provider"`
}

type MMSData struct {
	Country      string `json:"country"`
	Provider     string `json:"provider"`
	Bandwidth    string `json:"bandwidth"`
	ResponseTime string `json:"response_time"`
}

type VoiceCallData struct {
	Сountry             string  `json:"country"`
	Bandwidth           int     `json:"bandwidth"`
	ResponseTime        int     `json:"response_time"`
	Provider            string  `json:"provider"`
	ConnectionStability float32 `json:"connection_stability"`
	TTFB                int     `json:"ttfb"`
	VoicePurity         int     `json:"voice_purity"`
	MedianOfCallTime    int     `json:"median_of_call_time"`
}

type EmailData struct {
	Country      string `json:"country"`
	Provider     string `json:"provider"`
	DeliveryTime int    `json:"delivery_time"`
}

type BillingData struct {
	CreateCustomer bool `json:"create_customer"`
	Purchase       bool `json:"purchase"`
	Payout         bool `json:"payout"`
	Recurring      bool `json:"recurring"`
	FraudControl   bool `json:"fraud_control"`
	CheckoutPage   bool `json:"checkout_page"`
}

type SupportData struct {
	Topic         string `json:"topic"`
	ActiveTickets int    `json:"active_tickets"`
}

type IncidentData struct {
	Topic  string `json:"topic"`
	Status string `json:"status"` // возможные статусы active и closed
}

type ResultSetT struct {
	SMS       [][]SMSData     `json:"sms"`
	MMS       [][]MMSData     `json:"mms"`
	VoiceCall []VoiceCallData `json:"voice_call"`
	Email     [][]EmailData   `json:"email"` //map[string][][]EmailData
	Billing   BillingData     `json:"billing"`
	Support   []int           `json:"support"`
	Incidents []IncidentData  `json:"incident"`
}

type ResultT struct {
	Status bool       `json:"status"` // True, если все этапы сбора данных прошли успешно, False во всех остальных случаях
	Data   ResultSetT `json:"data"`   // Заполнен, если все этапы сбора  данных прошли успешно, nil во всех остальных случаях
	Error  string     `json:"error"`  // Пустая строка, если все этапы сбора данных прошли успешно, в случае ошибки заполнено текстом ошибки
}

func main() {
	// getResultData()
	// infoResult()
	SMSCollection = getResultData().SMS
	MMSCollection = getResultData().MMS
	VoiceCallCollection = getResultData().VoiceCall
	EmailCollection = getResultData().Email
	BillingCollection = getResultData().Billing
	SupportCollection = getResultData().Support
	IncidentsCollection = getResultData().Incidents
	TestCollection = infoResult()
	listenAndServeHTTP()
}

func listenAndServeHTTP() {
	router := mux.NewRouter()

	router.HandleFunc("/", handleConnection)
	router.HandleFunc("/sms", handleSMS).Methods("GET", "OPTIONS")
	router.HandleFunc("/mms", handleMMS).Methods("GET", "OPTIONS")
	router.HandleFunc("/voice_call", handleVoiceCall).Methods("GET", "OPTIONS")
	router.HandleFunc("/email", handleEmail).Methods("GET", "OPTIONS")
	router.HandleFunc("/billing", handleBilling).Methods("GET", "OPTIONS")
	router.HandleFunc("/support", handleSupport).Methods("GET", "OPTIONS")
	router.HandleFunc("/incidents", handleIncidents).Methods("GET", "OPTIONS")
	router.HandleFunc("/test", handleTest).Methods("GET", "OPTIONS")

	fmt.Println("Server listening on 8181")

	http.ListenAndServe("127.0.0.1:8181", router)
	fmt.Println("Listening on 8181")
}

func response(w http.ResponseWriter, r *http.Request, responseStruct interface{}) {
	response, _ := json.Marshal(responseStruct)

	w.Write(response)
}

func handleConnection(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "OK")
}

func handleSMS(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	response(w, r, SMSCollection)
}

func handleMMS(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	response(w, r, MMSCollection)
}

func handleVoiceCall(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	response(w, r, VoiceCallCollection)
}

func handleEmail(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	response(w, r, EmailCollection)
}

func handleBilling(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	response(w, r, BillingCollection)
}

func handleSupport(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	response(w, r, SupportCollection)
}

func handleIncidents(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	response(w, r, IncidentsCollection)
}

func handleTest(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	response(w, r, TestCollection)
}

func readSmsData(filePath string) []SMSData {
	f, err := os.Open(filePath)
	if err != nil {
		log.Fatal("Unable to read input file "+filePath, err)
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	records, err := csvReader.ReadAll()
	if err != nil {
		log.Fatal("Unable to parse file as CSV for "+filePath, err)
	}

	SMSDatas := splitCheckDataSMS(records)

	return SMSDatas
}

func splitCheckDataSMS(records [][]string) []SMSData {
	var SMSDatas []SMSData
	for _, v := range records {
		x := strings.Split(v[0], ";")
		if countries.ByName(x[0]) != countries.Unknown && len(x) == 4 && (x[3] == "Topolo" || x[3] == "Rond" || x[3] == "Kildy") {
			y := SMSData{Сountry: x[0], Bandwidth: x[1], ResponseTime: x[2], Provider: x[3]}
			SMSDatas = append(SMSDatas, y)
		}
	}
	return SMSDatas
}

func getMMS() []MMSData {
	client := http.Client{}
	resp, err := client.Get(mmsAddr)
	if err != nil {
		log.Fatalln(err)
	}

	textBytes, err := io.ReadAll(resp.Body) // Считываем ответ от сервера
	// fmt.Printf("\nКод ответа от сервера по данным MMS - %d\n", resp.StatusCode) // Узнаем код ответа от сервера
	var dataForMMS []MMSData
	if resp.StatusCode == 500 {
		fmt.Println("Произошла ошибка!")
		return dataForMMS
	}
	if err != nil {
		log.Fatalln(err)
	}

	defer resp.Body.Close()

	var dataFromResp []MMSData
	if err := json.Unmarshal(textBytes, &dataFromResp); err != nil { // Декодируем данные из ответа и записываем их в "dataFromResp"
		log.Fatalln(err)
	}

	for _, v := range dataFromResp {
		if countries.ByName(v.Country) != countries.Unknown && (v.Provider == "Topolo" || v.Provider == "Rond" || v.Provider == "Kildy") {
			dataForMMS = append(dataForMMS, v)
		}
	}
	return dataForMMS
}

func readVoiceData(filePath string) []VoiceCallData {
	f, err := os.Open(filePath)
	if err != nil {
		log.Fatal("Unable to read input file "+filePath, err)
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	records, err := csvReader.ReadAll()
	if err != nil {
		log.Fatal("Unable to parse file as CSV for "+filePath, err)
	}

	VoiceDatas := splitCheckDataVoice(records)

	return VoiceDatas
}

func splitCheckDataVoice(records [][]string) []VoiceCallData {
	var VoiceDatas []VoiceCallData
	for _, v := range records {
		x := strings.Split(v[0], ";")
		if countries.ByName(x[0]) != countries.Unknown && len(x) == 8 && (x[3] == "TransparentCalls" || x[3] == "E-Voice" || x[3] == "JustPhone") {
			x1, _ := strconv.Atoi(x[1])
			x2, _ := strconv.Atoi(x[2])
			x4, _ := strconv.ParseFloat(x[4], 32)
			x5, _ := strconv.Atoi(x[5])
			x6, _ := strconv.Atoi(x[6])
			x7, _ := strconv.Atoi(x[7])
			y := VoiceCallData{Сountry: x[0], Bandwidth: x1, ResponseTime: x2, Provider: x[3], ConnectionStability: float32(x4), TTFB: x5, VoicePurity: x6, MedianOfCallTime: x7}
			VoiceDatas = append(VoiceDatas, y)
		}
	}
	return VoiceDatas
}

func readEmailData(filePath string) []EmailData {
	f, err := os.Open(filePath)
	if err != nil {
		log.Fatal("Unable to read input file "+filePath, err)
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	records, err := csvReader.ReadAll()
	if err != nil {
		log.Fatal("Unable to parse file as CSV for "+filePath, err)
	}

	EmailDatas := splitCheckDataEmail(records)

	return EmailDatas
}

func splitCheckDataEmail(records [][]string) []EmailData {
	var EmailDatas []EmailData
	mapProviders := map[string]string{"Gmail": "Gmail", "Yahoo": "Yahoo", "Hotmail": "Hotmail", "MSN": "MSN", "Orange": "Orange", "Comcast": "Comcast", "AOL": "AOL", "Live": "Live", "RediffMail": "RediffMail", "GMX": "GMX", "Protonmail": "Protonmail", "Yandex": "Yandex", "Mail.ru": "Mail.ru"}
	for _, v := range records {
		x := strings.Split(v[0], ";")
		if len(x) == 3 {
			_, ok := mapProviders[x[1]]
			if countries.ByName(x[0]) != countries.Unknown && ok {
				x2, _ := strconv.Atoi(x[2])
				y := EmailData{Country: x[0], Provider: x[1], DeliveryTime: x2}
				EmailDatas = append(EmailDatas, y)
			}
		}
	}
	return EmailDatas
}

func readBillingData(filePath string) BillingData {
	f, err := os.Open(filePath)
	if err != nil {
		log.Fatal("Unable to read input file "+filePath, err)
	}
	defer f.Close()

	recordByte, err := io.ReadAll(f)
	if err != nil {
		log.Fatal("Unable to parse file as CSV for "+filePath, err)
	}

	var recordInt []int
	for _, v := range recordByte {
		x, _ := strconv.Atoi(string(v))
		recordInt = append(recordInt, x)
	}

	var arrayInt []int

	for i := len(recordInt) - 1; i >= 0; i-- {
		arrayInt = append(arrayInt, recordInt[i])
	}

	var sumInt uint8
	for i, v := range arrayInt {
		if v == 1 {
			sumInt += uint8(math.Pow(2, float64(i)))
		}
	}
	// fmt.Printf("\nПолучено десятичное число %d\n", sumInt)

	var boolData []bool
	for _, v := range arrayInt {
		var x bool
		if v == 1 {
			x = true
		} else {
			x = false
		}
		x = x && true
		boolData = append(boolData, x)
	}

	BillingData := BillingData{CreateCustomer: boolData[0], Purchase: boolData[1], Payout: boolData[2], Recurring: boolData[3], FraudControl: boolData[4], CheckoutPage: boolData[5]}
	return BillingData
}

func getSupportData() []SupportData {
	client := http.Client{}
	resp, err := client.Get(suppAddr)
	if err != nil {
		log.Fatalln(err)
	}

	textBytes, err := io.ReadAll(resp.Body) // Считываем ответ от сервера
	// fmt.Printf("\nКод ответа от сервера по данным SUPPORT - %d\n", resp.StatusCode) // Узнаем код ответа от сервера
	var dataForSup []SupportData
	if resp.StatusCode == 500 {
		fmt.Println("Произошла ошибка!")
		return dataForSup
	}
	if err != nil {
		log.Fatalln(err)
	}

	defer resp.Body.Close()

	if err := json.Unmarshal(textBytes, &dataForSup); err != nil { // Декодируем данные из ответа и записываем их в "dataFromResp"
		log.Fatalln(err)
	}
	return dataForSup
}

func getIncidentData() []IncidentData {
	client := http.Client{}
	resp, err := client.Get(incidentAddr)
	if err != nil {
		log.Fatalln(err)
	}

	textBytes, err := io.ReadAll(resp.Body) // Считываем ответ от сервера
	// fmt.Printf("\nКод ответа от сервера по данным INCIDENT - %d\n", resp.StatusCode) // Узнаем код ответа от сервера
	var dataForIncident []IncidentData
	if resp.StatusCode == 500 {
		fmt.Println("Произошла ошибка!")
		return dataForIncident
	}
	if err != nil {
		log.Fatalln(err)
	}

	defer resp.Body.Close()

	if err := json.Unmarshal(textBytes, &dataForIncident); err != nil { // Декодируем данные из ответа и записываем их в "dataFromResp"
		log.Fatalln(err)
	}

	return dataForIncident
}

func getResultData() ResultSetT {
	// Собираем данные SMS!!!
	smsData := readSmsData(smsDataLink)

	for i := range smsData { // Заменяем внутри среза код страны на полное название
		x := countries.ByName(string(smsData[i].Сountry))
		xStr := fmt.Sprintf("%v", x)
		smsData[i].Сountry = xStr
	}

	smsData1 := make([]SMSData, len(smsData))  // Создаем новый экземпляр массива
	copy(smsData1, smsData)                    // Полностью копируем все данные из одного массива в другой
	sort.Slice(smsData1, func(i, j int) bool { // Сортируем срез по названию провайдера
		return smsData1[i].Provider < smsData1[j].Provider
	})

	smsData2 := make([]SMSData, len(smsData))  // Создаем новый экземпляр массива
	copy(smsData2, smsData)                    // Полностью копируем все данные из одного массива в другой
	sort.Slice(smsData2, func(i, j int) bool { // Сортируем срез по названиям стран
		return smsData2[i].Сountry < smsData2[j].Сountry
	})

	var ResultSetT ResultSetT
	ResultSetT.SMS = append(ResultSetT.SMS, smsData1, smsData2)

	// Собираем данные MMS!!!

	mmsData := getMMS()
	for i := range mmsData {
		x := countries.ByName(string(mmsData[i].Country))
		xStr := fmt.Sprintf("%v", x)
		mmsData[i].Country = xStr
	}

	mmsData1 := make([]MMSData, len(mmsData))  // Создаем новый экземпляр массива
	copy(mmsData1, mmsData)                    // Полностью копируем все данные из одного массива в другой
	sort.Slice(mmsData1, func(i, j int) bool { // Сортируем срез по названию провайдера
		return mmsData1[i].Provider < mmsData1[j].Provider
	})

	mmsData2 := make([]MMSData, len(mmsData))  // Создаем новый экземпляр массива
	copy(mmsData2, mmsData)                    // Полностью копируем все данные из одного массива в другой
	sort.Slice(mmsData2, func(i, j int) bool { // Сортируем срез по названиям стран
		return mmsData2[i].Country < mmsData2[j].Country
	})

	ResultSetT.MMS = append(ResultSetT.MMS, mmsData1, mmsData2)

	// Собираем данные Voice Call!!!

	voiceData := readVoiceData(voiceDataLink)
	ResultSetT.VoiceCall = voiceData

	// Собираем данные Email!!!

	emailData := readEmailData(emailDataLink)

	sort.Slice(emailData, func(i, j int) bool { // Сортируем срез по названиям стран
		return emailData[i].DeliveryTime < emailData[j].DeliveryTime
	})

	useContryFast := make(map[string]string)          // карта для быстрых отработанных стран
	emailDataSortFast := make(map[string][]EmailData) // карта для самых быстрых

	for i := 0; i < len(emailData); i++ { // Ищем топ 3 самых БЫСТРЫХ провайдера в каждой стране
		count := 0
		nameUse := emailData[i].Country
		tempArray := make([]EmailData, 0, len(emailData))
		for j := 0; j < len(emailData); j++ {
			_, ok := useContryFast[nameUse]
			if ok {
				break
			}
			if !ok && emailData[i].Country == emailData[j].Country {
				tempArray = append(tempArray, emailData[j])
				count++
			}
			if count == 3 {
				emailDataSortFast[nameUse] = tempArray
				useContryFast[nameUse] = nameUse
				break
			}
		}
	}

	useContrySlow := make(map[string]string)          // карта для медленных отработанных стран
	emailDataSortSlow := make(map[string][]EmailData) // карта для самых медленных

	for i := len(emailData) - 1; i > -1; i-- { // Ищем топ 3 самых МЕДЛЕННЫХ провайдера в каждой стране
		count := 0
		nameUse := emailData[i].Country
		tempArray := make([]EmailData, 0, len(emailData))
		for j := len(emailData) - 1; j > -1; j-- {
			_, ok := useContrySlow[nameUse]
			if ok {
				break
			}
			if !ok && emailData[i].Country == emailData[j].Country {
				tempArray = append(tempArray, emailData[j])
				count++
			}
			if count == 3 {
				emailDataSortSlow[nameUse] = tempArray
				useContrySlow[nameUse] = nameUse
				break
			}
		}
	}

	var emailDataResult [][]EmailData //make(map[string][][]EmailData)

	keys := []string{}
	for key := range emailDataSortSlow {
		keys = append(keys, key)
	}

	for i := 0; i < len(keys); i++ {
		fast := emailDataSortFast[keys[i]]
		slow := emailDataSortSlow[keys[i]]
		// var tempArray [][]EmailData
		// tempArray = append(tempArray, fast, slow)
		// emailDataResult[keys[i]] = tempArray
		emailDataResult = append(emailDataResult, fast, slow)
	}
	ResultSetT.Email = emailDataResult

	// Собираем данные BILLING!!!

	billingData := readBillingData(billingDataLink)
	ResultSetT.Billing = billingData

	// Собираем данные SUPPORT!!!

	supportData := getSupportData()
	var medTime int = 60 / 18

	var sumTickets int
	for _, v := range supportData {
		sumTickets += v.ActiveTickets
	}

	var loadSupport int
	if sumTickets < 9 {
		loadSupport = 1
	} else if sumTickets >= 9 && sumTickets <= 16 {
		loadSupport = 2
	} else if sumTickets > 16 {
		loadSupport = 3
	}

	waitTime := sumTickets * medTime
	var supportDataResult []int
	supportDataResult = append(supportDataResult, loadSupport, waitTime)
	ResultSetT.Support = supportDataResult
	// if err != nil {
	// 	fmt.Printf("Error in write sms data: %s", err.Error())
	// }

	// Собираем данные INCIDENTS!!!

	incidentData := getIncidentData()

	sort.Slice(incidentData, func(i, j int) bool { // Сортируем срез по статусу
		return incidentData[i].Status < incidentData[j].Status
	})

	ResultSetT.Incidents = incidentData

	return ResultSetT
}

func infoResult() ResultT {
	var ResulT ResultT
	if len(getResultData().SMS) != 0 && len(getResultData().MMS) != 0 && len(getResultData().VoiceCall) != 0 && len(getResultData().Email) != 0 && len(getResultData().Support) != 0 && len(getResultData().Incidents) != 0 {
		ResulT.Status = true
		ResulT.Data = getResultData()
		ResulT.Error = ""
	} else {
		ResulT.Status = false
		xStr := fmt.Sprintf("%v", getResultData())
		ResulT.Error = "Error: " + xStr
	}
	return ResulT
}
