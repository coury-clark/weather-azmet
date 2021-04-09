package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"reflect"
	"strconv"
	"time"
)

func main() {

	current := time.Now()

	var year, station int
	flag.IntVar(&year, "y", current.Year(), "the year to fetch data between 2003 and current")
	flag.IntVar(&station, "s", int(PhoenixGreenway), "the weather station to fetch data for")
	flag.Parse()

	data, err := DownloadHourlyData(WeatherStation(station), year)
	if err != nil {
		log.Fatal("Error retrieving weather data.")
	}

	fmt.Println(data)
}

type HourlyWeatherData struct {
	Year                 int
	Day                  int
	Hour                 int
	AirTemperature       float32
	RelativeHumidity     float32
	VaporPressureDeficit float32
	SolarRadiation       float32
	Precipitation        float32
	SoilTempFourInches   float32
	SoilTempTwentyInches float32
	WindSpeedAverage     float32
	WindMagnitudeVector  float32
	WindDirectionVector  float32
	WindDirectionStdDev  float32
	WindSpeedMax         float32
	Evapotranspiration   float32
	VaporPressureActual  float32
	DewpointHourAverage  float32
	Time                 time.Time
}

type WeatherStation int

const (
	Aguila          WeatherStation = 7
	Bonita          WeatherStation = 9
	Bowie           WeatherStation = 33
	Buckeye         WeatherStation = 26
	Coolidge        WeatherStation = 5
	DesertRidge     WeatherStation = 27
	Harquahala      WeatherStation = 23
	Maricopa        WeatherStation = 6
	Mohave          WeatherStation = 20
	Mohave2         WeatherStation = 28
	FtMohave        WeatherStation = 40
	Paloma          WeatherStation = 19
	Parker          WeatherStation = 8
	Parker2         WeatherStation = 35
	Payson          WeatherStation = 32
	PhoenixGreenway WeatherStation = 12
	PhoenixEncanto  WeatherStation = 15
	QueenCreek      WeatherStation = 22
	Roll            WeatherStation = 24
	Safford         WeatherStation = 4
	Sahuarita       WeatherStation = 38
	Salome          WeatherStation = 41
	SanSimon        WeatherStation = 37
	Tucson          WeatherStation = 1
	Willcox         WeatherStation = 39
	YumaNorth       WeatherStation = 14
	YumaSouth       WeatherStation = 36
	YumaValley      WeatherStation = 2
)

func generateUrl(station WeatherStation, year int) string {
	urlFormat := "https://cals.arizona.edu/azmet/data/%d%srh.txt"
	yearStr := strconv.Itoa(year)
	return fmt.Sprintf(urlFormat, station, yearStr[len(yearStr)-2:])
}

func DownloadHourlyData(station WeatherStation, year int) ([]HourlyWeatherData, error) {

	if year < 2003 || year > 2099 {
		return []HourlyWeatherData{}, fmt.Errorf("invalid year to fetch Phoenix weather data: %d", year)
	}

	url := generateUrl(station, year)

	client := &http.Client{
		Timeout: time.Second * 10,
	}
	response, err := client.Get(url)

	if err != nil {
		return []HourlyWeatherData{}, err
	}

	return ReadHourlyData(response.Body)
}

func ReadHourlyData(reader io.ReadCloser) ([]HourlyWeatherData, error) {
	defer reader.Close()

	r := csv.NewReader(reader)
	data := make([]HourlyWeatherData, 0)
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return []HourlyWeatherData{}, err
		}
		rec, err := parseHourlyWeatherData(record)

		if err != nil {
			return []HourlyWeatherData{}, err
		}
		date, err := WeatherDataDate(rec)
		if err != nil {
			return []HourlyWeatherData{}, err
		}
		rec.Time = date
		data = append(data, rec)
	}

	return data, nil
}

func WeatherDataDate(data HourlyWeatherData) (time.Time, error) {
	tz, err := time.LoadLocation("America/Phoenix")
	if err != nil {
		return time.Time{}, fmt.Errorf("unable to resolve timezone")
	}
	firstOfYear := time.Date(data.Year, 1, 1, data.Hour, 0, 0, 0, tz)
	val := firstOfYear.Add(time.Hour * 24 * time.Duration(data.Day-1))
	return val, nil
}

func parseHourlyWeatherData(record []string) (HourlyWeatherData, error) {
	if len(record) != 18 {
		return HourlyWeatherData{}, fmt.Errorf("invalid field list length for hourly weather data, expecting 18 fields received %v", len(record))
	}

	var data HourlyWeatherData = HourlyWeatherData{}

	s := reflect.ValueOf(&data).Elem()

	for i := 0; i < 18; i++ {
		field := s.Field(i)
		if !field.CanSet() {
			return HourlyWeatherData{}, fmt.Errorf("field %s cannot be set", s.Type().Field(i).Name)
		}
		switch field.Type().Kind() {
		case reflect.Int:
			val, err := strconv.Atoi(record[i])
			if err != nil {
				return HourlyWeatherData{}, fmt.Errorf("unable to parse int type for value: %s", record[i])
			}
			field.Set(reflect.ValueOf(val))
		case reflect.Float32:
			val, err := strconv.ParseFloat(record[i], 32)
			if err != nil {
				return HourlyWeatherData{}, fmt.Errorf("unable to parse float32 type for value: %s", record[i])
			}
			field.Set(reflect.ValueOf(float32(val)))
		default:
			return HourlyWeatherData{}, fmt.Errorf("unable to parse type for field: %s", field.Type().String())
		}
	}

	return data, nil
}
