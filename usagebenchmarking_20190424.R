###EQW Usage Benchmarking File####

#Step 1: import all mapped market data
#Step 2: transform it to include popularity benchmarking at model level
#Step 3: format and output

#Note: in _test3, we remove the age requirement for calculation


library(plyr)
library(dplyr)
library(tidyr)
library(reshape2)
library(readr)
library(lubridate)
library(RPostgreSQL)
library(data.table)

pw<-{"eqwAnalystRO2018"}
drv <- dbDriver(drvName = "PostgreSQL")
eqw <- dbConnect(drv, dbname = "eqw",
                 host = "db.equipmentwatchapi.com", port = 5432,
                 user = "eqw_analyst_ro", password = pw)

raw <- dbGetQuery(eqw,'Select md."fromDate",md."toDate",md."modelYear",
                  md."serialNumber",md."meterReads",md."condition",md."modelId",md."stateCode", md."countryCode",
                  ca."categoryId",ca."categoryName",st."subtypeId",st."subtypeName",
                  sc."sizeClassId",sc."sizeClassName",ma."manufacturerId"
                  from staging."marketData" md
                  join staging."models" mo on md."modelId"=mo."modelId" 
                  join staging."manufacturers" ma on mo."manufacturerId"=ma."manufacturerId"
                  join staging."sizes" sc on mo."sizeClassId"=sc."sizeClassId"
                  join staging."subtypes" st on sc."subtypeId"=st."subtypeId"
                  join staging."categories" ca on st."categoryId"=ca."categoryId"
                  where md."saleType" like \'R%\' and md."countryCode" in (\'US\',\'CA\') 
                  and md."modelYear" > 0
                  and md."toDate" >= now() - INTERVAL \'12 MONTHS\' 
                  and md."toDate" <= now()
                  and md."meterReads" is not null
                  ')


val<- dbGetQuery(eqw,'SELECT DISTINCT ca."categoryId",ca."categoryName",st."subtypeId",st."subtypeName",sz."sizeClassId",sz."sizeClassName", ma."manufacturerId",ma."manufacturerName",mo."modelName",mo."modelId"
                 FROM staging."values" va
                 JOIN staging."models" mo
                 ON mo."modelId" = va."modelId"
                 JOIN staging."sizes" sz
                 ON sz."sizeClassId" = mo."sizeClassId"
                 JOIN staging."subtypes" st
                 ON st."subtypeId" = sz."subtypeId"
                 JOIN staging."categories" ca
                 ON ca."categoryId"=st."categoryId"
                 JOIN staging."manufacturers" ma
                 ON ma."manufacturerId" = mo."manufacturerId"
                 WHERE mo."surveyFlag" = 1')


###Balers; Articulated Rear dumps

#raw2<-fread("G:/EQWAtlanta/Data Analytics/EQW Market Popularity/2018_Market_Data.csv", stringsAsFactors=F)
#US&Canada: 5,427,632 Total: 5,720,218 5.4% outside of North America
#MeterReads NULL: 2,061,940;  0: 1 ==> 3,365,691


#names(raw)<-c('startDate','endDate','modelYear','serialNumber','meterReads','condition','modelId','state','stateCode',
#              'country','countryCode','category','subtype','manufacturer','model','sizeClass')

#raw$state<-NULL
#raw$country<-NULL
#names(raw)<-c('startDate','endDate','modelYear','serialNumber','meterReads','condition','modelId','stateCode',
#              'countryCode','category','subtype','manufacturer','model','sizeClass')

all<-subset(raw, !is.na(raw$categoryName) & raw$sizeClassName!='UNKNOWN')#3,254,323

all<-all[!grepl("M",all$meterReads),]#2,896,862 -- removing miles uom
all<-all[!grepl("^\\d+$",all$meterReads),]#2,896,524 removing meterReads without uom

all$meterReads<-parse_number(all$meterReads)
all$modelYear<-parse_number(all$modelYear)

all<-subset(all, !is.na(all$meterReads) & !is.na(all$modelYear)) #2,447,649

all<-subset(all, !(all$categoryName %in% c('On-Highway Trucks', 'Light Truck','Autos & Vehicles','Auto','On-Highway Trucks - Light Duty', #2,405,509
                                       'On-Highway Trucks - Medium Duty','On-Highway Trucks - Heavy Duty','Trucks')))
all<-all[!all$categoryId==1847,] #balers


###balers; rear dumps
#reardump<-raw[which(raw$subtypeId==84),] #65,781
#length(reardump$meterReads[grepl("M",reardump$meterReads)])#120
#nrow(reardump[grepl("M",reardump$meterReads),])
#balers<-raw[which(raw$subtypeId==1847),] #18034
#unique(raw$meterReads[(grepl("[A-Za-z]*",raw$meterReads))])
#nonhour<-raw[(grepl("[A-Za-z]*",raw$meterReads) & !grepl("H",raw$meterReads)),]
#unique(nonhour$categoryName)
#length(raw$meterReads[grepl("^\\d+$",raw$meterReads)]) #338
#num<-raw[grepl("^\\d+$",raw$meterReads),] #338

#nonhour$meterReads[(grepl("[A-Za-z]*",nonhour$meterReads) & !grepl("M",nonhour$meterReads))]

#uom<-unique(gsub("\\d+?\\s?([A-Za-z])","\\1",nonhour$meterReads))



all$meterReads<-as.numeric(all$meterReads)
all$modelYear<-as.numeric(all$modelYear)

all<-subset(all, !is.na(all$meterReads))
all<-subset(all, !is.na(all$modelId))
all<-subset(all, !is.na(all$modelYear))
#2,951,597

#all<-subset(all, modelYear > 1989 & modelYear < 2018)
all<-subset(all, modelYear >= 1990 & modelYear <= 2019) #2,346,096
#2,866,830 (1991-2018) 2,


all<-subset(all, !(all$meterReads <10 )) #2,294,992
#2,773,559 -> 2,789,532
all<-subset(all, !(all$meterReads >1000000))#2,294,941
#2,769,745 -> 2,785,700
all<-subset(all, !(all$meterReads == 0 ))

#all$endDate2<-as.Date.factor(all$endDate, format="%m/%d/%Y")
#all$endDate2<-as.Date(all$endDate, format="%m/%d/%Y")
all$toDate2<-as.Date(all$toDate)

all<-subset(all, !is.na(all$toDate2))
#2,769,745
#all$avgAnnUse<-all$meterReads/(year(all$toDate2)-all$modelYear)
#all$avgAnnUse<-all$meterReads/(2019-all$modelYear)

#all<-subset(all, avgAnnUse>50 & avgAnnUse!="Inf")
#2,488,971 -> 2,503,814


###################################
#Creating usage benchmarks at the Size Class level
####################################

#all$sizeClassConcat<-paste0(all$category, all$subtype, all$sizeClass)

sclist<-unique(all$sizeClassId)
start_time_sct<-Sys.time()

for(i in 1:length(sclist)){
  
  tempmods<-subset(all, all$sizeClassId==sclist[i])
  modcount<-as.numeric(nrow(tempmods))
  
  #Create the age filter
  tempmods$age<-2019-as.numeric(tempmods$modelYear)
  
  agelist<-unique(tempmods$age)
  
  #Start here once the VINs have been run!!!!
  
  #Subset each calculation by model age, where the quantiles are created
  for(a in 1:length(agelist)){
    
    tempdf2<-subset(tempmods,tempmods$age==agelist[a])
    
    #tempdf2$meanAnnualUsage[1]<-mean(tempdf2$avgAnnUse)
    tempdf2$meanAnnualUsage[1]<-mean(tempdf2$meterReads)
    tempdf2$benchmarkLevel[1]<-"sizeClass"
    
    
    tempdf2$percentile5[1]<-quantile(tempdf2$meterReads, 0.05)[[1]]
    tempdf2$percentile10[1]<-quantile(tempdf2$meterReads, 0.10)[[1]]
    tempdf2$percentile15[1]<-quantile(tempdf2$meterReads, 0.15)[[1]]
    tempdf2$percentile20[1]<-quantile(tempdf2$meterReads, 0.20)[[1]]
    tempdf2$percentile25[1]<-quantile(tempdf2$meterReads, 0.25)[[1]]
    tempdf2$percentile30[1]<-quantile(tempdf2$meterReads, 0.30)[[1]]
    tempdf2$percentile35[1]<-quantile(tempdf2$meterReads, 0.35)[[1]]
    tempdf2$percentile40[1]<-quantile(tempdf2$meterReads, 0.40)[[1]]
    tempdf2$percentile45[1]<-quantile(tempdf2$meterReads, 0.45)[[1]]
    tempdf2$percentile50[1]<-quantile(tempdf2$meterReads, 0.50)[[1]]
    tempdf2$percentile55[1]<-quantile(tempdf2$meterReads, 0.55)[[1]]
    tempdf2$percentile60[1]<-quantile(tempdf2$meterReads, 0.60)[[1]]
    tempdf2$percentile65[1]<-quantile(tempdf2$meterReads, 0.65)[[1]]
    tempdf2$percentile70[1]<-quantile(tempdf2$meterReads, 0.70)[[1]]
    tempdf2$percentile75[1]<-quantile(tempdf2$meterReads, 0.75)[[1]]
    tempdf2$percentile80[1]<-quantile(tempdf2$meterReads, 0.80)[[1]]
    tempdf2$percentile85[1]<-quantile(tempdf2$meterReads, 0.85)[[1]]
    tempdf2$percentile90[1]<-quantile(tempdf2$meterReads, 0.90)[[1]]
    tempdf2$percentile95[1]<-quantile(tempdf2$meterReads, 0.95)[[1]]
    tempdf2$percentile99[1]<-quantile(tempdf2$meterReads, 0.99)[[1]]
    
    
    disttest5<-tempdf2$meterReads>tempdf2$percentile5[1]
    tempdf2$distribution5[1]<-sum(disttest5==T)
    
    disttest10<-tempdf2$meterReads>tempdf2$percentile10[1]
    tempdf2$distribution10[1]<-sum(disttest10==T)
    
    disttest15<-tempdf2$meterReads>tempdf2$percentile15[1]
    tempdf2$distribution15[1]<-sum(disttest15==T)
    
    disttest20<-tempdf2$meterReads>tempdf2$percentile20[1]
    tempdf2$distribution20[1]<-sum(disttest20==T)
    
    disttest25<-tempdf2$meterReads>tempdf2$percentile25[1]
    tempdf2$distribution25[1]<-sum(disttest25==T)
    
    disttest30<-tempdf2$meterReads>tempdf2$percentile30[1]
    tempdf2$distribution30[1]<-sum(disttest30==T)
    
    disttest35<-tempdf2$meterReads>tempdf2$percentile35[1]
    tempdf2$distribution35[1]<-sum(disttest35==T)
    
    disttest40<-tempdf2$meterReads>tempdf2$percentile40[1]
    tempdf2$distribution40[1]<-sum(disttest40==T)
    
    disttest45<-tempdf2$meterReads>tempdf2$percentile45[1]
    tempdf2$distribution45[1]<-sum(disttest45==T)
    
    disttest50<-tempdf2$meterReads>tempdf2$percentile50[1]
    tempdf2$distribution50[1]<-sum(disttest50==T)
    
    disttest55<-tempdf2$meterReads>tempdf2$percentile55[1]
    tempdf2$distribution55[1]<-sum(disttest55==T)
    
    disttest60<-tempdf2$meterReads>tempdf2$percentile60[1]
    tempdf2$distribution60[1]<-sum(disttest60==T)
    
    disttest65<-tempdf2$meterReads>tempdf2$percentile65[1]
    tempdf2$distribution65[1]<-sum(disttest65==T)
    
    disttest70<-tempdf2$meterReads>tempdf2$percentile70[1]
    tempdf2$distribution70[1]<-sum(disttest70==T)
    
    disttest75<-tempdf2$meterReads>tempdf2$percentile75[1]
    tempdf2$distribution75[1]<-sum(disttest75==T)
    
    disttest80<-tempdf2$meterReads>tempdf2$percentile80[1]
    tempdf2$distribution80[1]<-sum(disttest80==T)
    
    disttest85<-tempdf2$meterReads>tempdf2$percentile85[1]
    tempdf2$distribution85[1]<-sum(disttest85==T)
    
    disttest90<-tempdf2$meterReads>tempdf2$percentile90[1]
    tempdf2$distribution90[1]<-sum(disttest90==T)
    
    disttest95<-tempdf2$meterReads>tempdf2$percentile95[1]
    tempdf2$distribution95[1]<-sum(disttest95==T)
    
    disttest99<-tempdf2$meterReads>tempdf2$percentile99[1]
    tempdf2$distribution99[1]<-sum(disttest99==T)
    
    tempdf2$recordCount<-nrow(tempdf2)
    
    if(i==1 & a==1){sctable<-tempdf2[1,]}
    else{sctable<-rbind(sctable, tempdf2[1,])}
    
    print(tail(sctable$sizeClassId,1))
  }
}
end_time_sct<-Sys.time()
run_time_sct<-end_time_sct-start_time_sct

###################################
#Creating usage benchmarks at the Subtype level
####################################

#all$subtypeConcat<-paste0(all$category, all$subtype)

sublist<-unique(all$subtypeId)

start_time_sub<-Sys.time()
for(i in 1:length(sublist)){
  
  tempmods<-subset(all, all$subtypeId==sublist[i])
  modcount<-as.numeric(nrow(tempmods))
  
  #Create the age filter
  tempmods$age<-2019-as.numeric(tempmods$modelYear)
  
  agelist<-unique(tempmods$age)
  
  #Start here once the VINs have been run!!!!
  
  #Subset each calculation by model age, where the quantiles are created
  for(a in 1:length(agelist)){
    
    tempdf2<-subset(tempmods,tempmods$age==agelist[a])
    
    #tempdf2$meanAnnualUsage[1]<-mean(tempdf2$avgAnnUse)
    tempdf2$meanAnnualUsage[1]<-mean(tempdf2$meterReads)
    tempdf2$benchmarkLevel[1]<-"subtype"
    
    tempdf2$percentile5[1]<-quantile(tempdf2$meterReads, 0.05)[[1]]
    tempdf2$percentile10[1]<-quantile(tempdf2$meterReads, 0.10)[[1]]
    tempdf2$percentile15[1]<-quantile(tempdf2$meterReads, 0.15)[[1]]
    tempdf2$percentile20[1]<-quantile(tempdf2$meterReads, 0.20)[[1]]
    tempdf2$percentile25[1]<-quantile(tempdf2$meterReads, 0.25)[[1]]
    tempdf2$percentile30[1]<-quantile(tempdf2$meterReads, 0.30)[[1]]
    tempdf2$percentile35[1]<-quantile(tempdf2$meterReads, 0.35)[[1]]
    tempdf2$percentile40[1]<-quantile(tempdf2$meterReads, 0.40)[[1]]
    tempdf2$percentile45[1]<-quantile(tempdf2$meterReads, 0.45)[[1]]
    tempdf2$percentile50[1]<-quantile(tempdf2$meterReads, 0.50)[[1]]
    tempdf2$percentile55[1]<-quantile(tempdf2$meterReads, 0.55)[[1]]
    tempdf2$percentile60[1]<-quantile(tempdf2$meterReads, 0.60)[[1]]
    tempdf2$percentile65[1]<-quantile(tempdf2$meterReads, 0.65)[[1]]
    tempdf2$percentile70[1]<-quantile(tempdf2$meterReads, 0.70)[[1]]
    tempdf2$percentile75[1]<-quantile(tempdf2$meterReads, 0.75)[[1]]
    tempdf2$percentile80[1]<-quantile(tempdf2$meterReads, 0.80)[[1]]
    tempdf2$percentile85[1]<-quantile(tempdf2$meterReads, 0.85)[[1]]
    tempdf2$percentile90[1]<-quantile(tempdf2$meterReads, 0.90)[[1]]
    tempdf2$percentile95[1]<-quantile(tempdf2$meterReads, 0.95)[[1]]
    tempdf2$percentile99[1]<-quantile(tempdf2$meterReads, 0.99)[[1]]
    
    
    disttest5<-tempdf2$meterReads>tempdf2$percentile5[1]
    tempdf2$distribution5[1]<-sum(disttest5==T)
    
    disttest10<-tempdf2$meterReads>tempdf2$percentile10[1]
    tempdf2$distribution10[1]<-sum(disttest10==T)
    
    disttest15<-tempdf2$meterReads>tempdf2$percentile15[1]
    tempdf2$distribution15[1]<-sum(disttest15==T)
    
    disttest20<-tempdf2$meterReads>tempdf2$percentile20[1]
    tempdf2$distribution20[1]<-sum(disttest20==T)
    
    disttest25<-tempdf2$meterReads>tempdf2$percentile25[1]
    tempdf2$distribution25[1]<-sum(disttest25==T)
    
    disttest30<-tempdf2$meterReads>tempdf2$percentile30[1]
    tempdf2$distribution30[1]<-sum(disttest30==T)
    
    disttest35<-tempdf2$meterReads>tempdf2$percentile35[1]
    tempdf2$distribution35[1]<-sum(disttest35==T)
    
    disttest40<-tempdf2$meterReads>tempdf2$percentile40[1]
    tempdf2$distribution40[1]<-sum(disttest40==T)
    
    disttest45<-tempdf2$meterReads>tempdf2$percentile45[1]
    tempdf2$distribution45[1]<-sum(disttest45==T)
    
    disttest50<-tempdf2$meterReads>tempdf2$percentile50[1]
    tempdf2$distribution50[1]<-sum(disttest50==T)
    
    disttest55<-tempdf2$meterReads>tempdf2$percentile55[1]
    tempdf2$distribution55[1]<-sum(disttest55==T)
    
    disttest60<-tempdf2$meterReads>tempdf2$percentile60[1]
    tempdf2$distribution60[1]<-sum(disttest60==T)
    
    disttest65<-tempdf2$meterReads>tempdf2$percentile65[1]
    tempdf2$distribution65[1]<-sum(disttest65==T)
    
    disttest70<-tempdf2$meterReads>tempdf2$percentile70[1]
    tempdf2$distribution70[1]<-sum(disttest70==T)
    
    disttest75<-tempdf2$meterReads>tempdf2$percentile75[1]
    tempdf2$distribution75[1]<-sum(disttest75==T)
    
    disttest80<-tempdf2$meterReads>tempdf2$percentile80[1]
    tempdf2$distribution80[1]<-sum(disttest80==T)
    
    disttest85<-tempdf2$meterReads>tempdf2$percentile85[1]
    tempdf2$distribution85[1]<-sum(disttest85==T)
    
    disttest90<-tempdf2$meterReads>tempdf2$percentile90[1]
    tempdf2$distribution90[1]<-sum(disttest90==T)
    
    disttest95<-tempdf2$meterReads>tempdf2$percentile95[1]
    tempdf2$distribution95[1]<-sum(disttest95==T)
    
    disttest99<-tempdf2$meterReads>tempdf2$percentile99[1]
    tempdf2$distribution99[1]<-sum(disttest99==T)
    
    tempdf2$recordCount<-nrow(tempdf2)
    
    if(i==1 & a==1){subtable<-tempdf2[1,]}
    else{subtable<-rbind(subtable, tempdf2[1,])}
    
    print(tail(subtable$subtypeId,1))
  }
}

end_time_sub<-Sys.time()
run_time_sub<-end_time_sub-start_time_sub

###################################
#Creating usage benchmarks at the Category level
####################################

catlist<-unique(all$categoryId)
start_time_cat<-Sys.time()
for(i in 1:length(catlist)){
  
  tempmods<-subset(all, all$categoryId==catlist[i])
  modcount<-as.numeric(nrow(tempmods))
  
  #Create the age filter
  tempmods$age<-2019-as.numeric(tempmods$modelYear)
  
  agelist<-unique(tempmods$age)
  
  #Start here once the VINs have been run!!!!
  
  #Subset each calculation by model age, where the quantiles are created
  for(a in 1:length(agelist)){
    
    tempdf2<-subset(tempmods,tempmods$age==agelist[a])
    
    #tempdf2$meanAnnualUsage[1]<-mean(tempdf2$avgAnnUse)
    tempdf2$meanAnnualUsage[1]<-mean(tempdf2$meterReads)
    tempdf2$benchmarkLevel[1]<-"category"
    
    tempdf2$percentile5[1]<-quantile(tempdf2$meterReads, 0.05)[[1]]
    tempdf2$percentile10[1]<-quantile(tempdf2$meterReads, 0.10)[[1]]
    tempdf2$percentile15[1]<-quantile(tempdf2$meterReads, 0.15)[[1]]
    tempdf2$percentile20[1]<-quantile(tempdf2$meterReads, 0.20)[[1]]
    tempdf2$percentile25[1]<-quantile(tempdf2$meterReads, 0.25)[[1]]
    tempdf2$percentile30[1]<-quantile(tempdf2$meterReads, 0.30)[[1]]
    tempdf2$percentile35[1]<-quantile(tempdf2$meterReads, 0.35)[[1]]
    tempdf2$percentile40[1]<-quantile(tempdf2$meterReads, 0.40)[[1]]
    tempdf2$percentile45[1]<-quantile(tempdf2$meterReads, 0.45)[[1]]
    tempdf2$percentile50[1]<-quantile(tempdf2$meterReads, 0.50)[[1]]
    tempdf2$percentile55[1]<-quantile(tempdf2$meterReads, 0.55)[[1]]
    tempdf2$percentile60[1]<-quantile(tempdf2$meterReads, 0.60)[[1]]
    tempdf2$percentile65[1]<-quantile(tempdf2$meterReads, 0.65)[[1]]
    tempdf2$percentile70[1]<-quantile(tempdf2$meterReads, 0.70)[[1]]
    tempdf2$percentile75[1]<-quantile(tempdf2$meterReads, 0.75)[[1]]
    tempdf2$percentile80[1]<-quantile(tempdf2$meterReads, 0.80)[[1]]
    tempdf2$percentile85[1]<-quantile(tempdf2$meterReads, 0.85)[[1]]
    tempdf2$percentile90[1]<-quantile(tempdf2$meterReads, 0.90)[[1]]
    tempdf2$percentile95[1]<-quantile(tempdf2$meterReads, 0.95)[[1]]
    tempdf2$percentile99[1]<-quantile(tempdf2$meterReads, 0.99)[[1]]
    
    
    disttest5<-tempdf2$meterReads>tempdf2$percentile5[1]
    tempdf2$distribution5[1]<-sum(disttest5==T)
    
    disttest10<-tempdf2$meterReads>tempdf2$percentile10[1]
    tempdf2$distribution10[1]<-sum(disttest10==T)
    
    disttest15<-tempdf2$meterReads>tempdf2$percentile15[1]
    tempdf2$distribution15[1]<-sum(disttest15==T)
    
    disttest20<-tempdf2$meterReads>tempdf2$percentile20[1]
    tempdf2$distribution20[1]<-sum(disttest20==T)
    
    disttest25<-tempdf2$meterReads>tempdf2$percentile25[1]
    tempdf2$distribution25[1]<-sum(disttest25==T)
    
    disttest30<-tempdf2$meterReads>tempdf2$percentile30[1]
    tempdf2$distribution30[1]<-sum(disttest30==T)
    
    disttest35<-tempdf2$meterReads>tempdf2$percentile35[1]
    tempdf2$distribution35[1]<-sum(disttest35==T)
    
    disttest40<-tempdf2$meterReads>tempdf2$percentile40[1]
    tempdf2$distribution40[1]<-sum(disttest40==T)
    
    disttest45<-tempdf2$meterReads>tempdf2$percentile45[1]
    tempdf2$distribution45[1]<-sum(disttest45==T)
    
    disttest50<-tempdf2$meterReads>tempdf2$percentile50[1]
    tempdf2$distribution50[1]<-sum(disttest50==T)
    
    disttest55<-tempdf2$meterReads>tempdf2$percentile55[1]
    tempdf2$distribution55[1]<-sum(disttest55==T)
    
    disttest60<-tempdf2$meterReads>tempdf2$percentile60[1]
    tempdf2$distribution60[1]<-sum(disttest60==T)
    
    disttest65<-tempdf2$meterReads>tempdf2$percentile65[1]
    tempdf2$distribution65[1]<-sum(disttest65==T)
    
    disttest70<-tempdf2$meterReads>tempdf2$percentile70[1]
    tempdf2$distribution70[1]<-sum(disttest70==T)
    
    disttest75<-tempdf2$meterReads>tempdf2$percentile75[1]
    tempdf2$distribution75[1]<-sum(disttest75==T)
    
    disttest80<-tempdf2$meterReads>tempdf2$percentile80[1]
    tempdf2$distribution80[1]<-sum(disttest80==T)
    
    disttest85<-tempdf2$meterReads>tempdf2$percentile85[1]
    tempdf2$distribution85[1]<-sum(disttest85==T)
    
    disttest90<-tempdf2$meterReads>tempdf2$percentile90[1]
    tempdf2$distribution90[1]<-sum(disttest90==T)
    
    disttest95<-tempdf2$meterReads>tempdf2$percentile95[1]
    tempdf2$distribution95[1]<-sum(disttest95==T)
    
    disttest99<-tempdf2$meterReads>tempdf2$percentile99[1]
    tempdf2$distribution99[1]<-sum(disttest99==T)
    
    tempdf2$recordCount<-nrow(tempdf2)
    
    if(i==1& a==1){cattable<-tempdf2[1,]}
    else{cattable<-rbind(cattable, tempdf2[1,])}
    
    print(tail(cattable$categoryId,1))
  }
}
end_time_cat<-Sys.time()
run_time_cat<-end_time_cat-start_time_cat

unq_sct<-sctable[!duplicated(sctable),] #11,160
unq_sub<-subtable[!duplicated(subtable),] #4,143
unq_cat<-cattable[!duplicated(cattable),] #924

#modtable2<-subset(modtable, modtable$recordCount>50)
sctable2<-subset(unq_sct, unq_sct$recordCount>29)
subtable2<-subset(unq_sub, unq_sub$recordCount>29)

cattable2<-subset(unq_cat, unq_cat$recordCount>36)
#cattable2<-unq_cat

#modtable2<-modtable
#sctable2<-sctable
#subtable2<-subtable
#cattable2<-cattable

#Filtering and creating lookup
#modrejects<-filter(modtable, !(modelId %in% modtable2$modelId))
#modrejects2<-select(modrejects, modelId, category, subtype, sizeClass)
#modrejects2$sizeClassConcat<-paste0(modrejects2$category, modrejects2$subtype, modrejects2$sizeClass)

#Final table for modelIds with sizeClass benchmarks
#sctable3<-merge(modrejects2, sctable2, by.x="sizeClassConcat", by.y="sizeClassConcat")


#####Repeat for subtypes######
#names(sctable3)[2]<-c("modelId")
#screjects<-filter(sctable,!(modelId %in% modtable2$modelId & modelId %in% sctable3$modelId))
#screjects2<-select(screjects, modelId, category, subtype, sizeClass)
#screjects2$subtypeConcat<-paste0(screjects2$category, screjects2$subtype)

#subtable3<-merge(screjects2,subtable2,by="subtypeConcat")

sctable3<-merge(sctable2, val, by="sizeClassId")
subtable3<-merge(subtable2, val, by="subtypeId")
cattable3<-merge(cattable2, val, by="categoryId")

######Repeat for categories######
#names(subtable3)[2]<-c("modelId")
#subrejects<-filter(subtable,!(modelId %in% modtable2$modelId & modelId %in% sctable3$modelId & modelId %in% subtable3$modelId))
#subrejects2<-select(subrejects, modelId, category, subtype, sizeClass)

#cattable3<-merge(subrejects2,cattable2,by="category")

#names(cattable3)[2]<-c("modelId")


######Clean dataframes so field names match#####
#modtable4<-select(modtable2, modelId, modelYear, age, benchmarkLevel, meanAnnualUsage,recordCount, percentile5, percentile10, percentile15, percentile20,
#                  percentile25, percentile30, percentile35, percentile40, percentile45, percentile50, percentile55, percentile60,
#                  percentile65, percentile70, percentile75, percentile80, percentile85, percentile90, percentile95, percentile99, 
#                  distribution5, distribution10, distribution15, distribution20, distribution25, distribution30, distribution35,
#                  distribution40, distribution45, distribution50, distribution55, distribution60, distribution65, distribution70, 
#                  distribution75, distribution80, distribution85, distribution90, distribution95, distribution99)
#modtable4<-modtable4[!duplicated(modtable4$model),]
#length(unique(modtable4$modelId))

sctable4<-select(sctable3, modelId.y, modelYear, age, benchmarkLevel, meanAnnualUsage,recordCount, percentile5, percentile10, percentile15, percentile20,
                 percentile25, percentile30, percentile35, percentile40, percentile45, percentile50, percentile55, percentile60,
                 percentile65, percentile70, percentile75, percentile80, percentile85, percentile90, percentile95, percentile99, 
                 distribution5, distribution10, distribution15, distribution20, distribution25, distribution30, distribution35,
                 distribution40, distribution45, distribution50, distribution55, distribution60, distribution65, distribution70, 
                 distribution75, distribution80, distribution85, distribution90, distribution95, distribution99)

subtable4<-select(subtable3, modelId.y, modelYear, age, benchmarkLevel, meanAnnualUsage,recordCount, percentile5, percentile10, percentile15, percentile20,
                  percentile25, percentile30, percentile35, percentile40, percentile45, percentile50, percentile55, percentile60,
                  percentile65, percentile70, percentile75, percentile80, percentile85, percentile90, percentile95, percentile99, 
                  distribution5, distribution10, distribution15, distribution20, distribution25, distribution30, distribution35,
                  distribution40, distribution45, distribution50, distribution55, distribution60, distribution65, distribution70, 
                  distribution75, distribution80, distribution85, distribution90, distribution95, distribution99)

cattable4<-select(cattable3, modelId.y, modelYear, age, benchmarkLevel, meanAnnualUsage,recordCount, percentile5, percentile10, percentile15, percentile20,
                  percentile25, percentile30, percentile35, percentile40, percentile45, percentile50, percentile55, percentile60,
                  percentile65, percentile70, percentile75, percentile80, percentile85, percentile90, percentile95, percentile99, 
                  distribution5, distribution10, distribution15, distribution20, distribution25, distribution30, distribution35,
                  distribution40, distribution45, distribution50, distribution55, distribution60, distribution65, distribution70, 
                  distribution75, distribution80, distribution85, distribution90, distribution95, distribution99)

colnames(sctable4)[1]<-c("modelId")
colnames(subtable4)[1]<-c("modelId")
colnames(cattable4)[1]<-c("modelId")

###Rbind level "3" tables#########

finaldf<-rbind(sctable4,subtable4,cattable4)



###Final QA and write.csv#######
#Test the dedupe theory!!######
length(unique(finaldf$modelId)) #22,545
#length(unique(modtable4$modelId))
length(unique(sctable4$modelId))+length(unique(subtable4$modelId))+length(unique(cattable4$modelId))-length(unique(finaldf$modelId)) #39,081

#Benchmark level assignment check
unique(sctable4$benchmarkLevel)
unique(subtable4$benchmarkLevel)
unique(cattable4$benchmarkLevel)

#Record count check for each benchmark level
#ifelse(unique(finaldf$recordCount[finaldf$benchmarkLevel=="model"]>50)==TRUE,"Pass","Fail")
#ifelse(unique(finaldf$recordCount[finaldf$benchmarkLevel=="sizeClass"]>75)==TRUE,"Pass","Fail")
#ifelse(unique(finaldf$recordCount[finaldf$benchmarkLevel=="subtype"]>100)==TRUE,"Pass","Fail")
#ifelse(unique(finaldf$recordCount[finaldf$benchmarkLevel=="category"]>150)==TRUE,"Pass","Fail")



###Drop unneeded fields###

finaldf2<-select(finaldf, modelId, modelYear, age, benchmarkLevel, meanAnnualUsage,recordCount, percentile5, percentile10, percentile15, percentile20,
                 percentile25, percentile30, percentile35, percentile40, percentile45, percentile50, percentile55, percentile60, 
                 percentile65, percentile70, percentile75, percentile80, percentile85, percentile90, percentile95, percentile99,
                 distribution5, distribution10, distribution15, distribution20, distribution25, distribution30, distribution35, 
                 distribution40, distribution45, distribution50, distribution55, distribution60, distribution65, distribution70,
                 distribution75, distribution80, distribution85, distribution90, distribution95, distribution99)

###Assigning Priorities for Deduping###

finaldf2$benchmarkPriority<-NA

table(finaldf2$benchmarkLevel)

#finaldf2$benchmarkPriority[finaldf2$benchmarkLevel=="model"]<-1
finaldf2$benchmarkPriority[finaldf2$benchmarkLevel=="sizeClass"]<-1
finaldf2$benchmarkPriority[finaldf2$benchmarkLevel=="subtype"]<-2
finaldf2$benchmarkPriority[finaldf2$benchmarkLevel=="category"]<-3


#modtable4<-subset(finaldf2, modtable$recordCount>50)
#sctable4<-subset(finaldf2, sctable$recordCount>75)
#subtable4<-subset(finaldf2, subtable$recordCount>100)
#cattable4<-subset(finaldf2, cattable$recordCount>150)

#finalmods<-filter(finaldf2, benchmarkLevel=="model")
#finalsc<-filter(finaldf2, benchmarkLevel=="sizeClass")
#finalsub<-filter(finaldf2, benchmarkLevel=="subtype")
#finalcat<-filter(finaldf2, benchmarkLevel=="category")


#Filtered by recordCounts Already
#finalmods2<-filter(finalmods, modelId %in% modtable4$modelId)
#finalsc2<-filter(finalsc, modelId %in% sctable4$modelId)
#finalsub2<-filter(finalsub, modelId %in% subtable4$modelId)
#finalcat2<-filter(finalcat, modelId %in% cattable4$modelId)

#modsrejects<-filter(finalmods, !(modelId %in% finalmods2$modelId))
#modsrejects2<-select(modsrejects, modelId)


#finaldf2$idyear<-paste0(finaldf2$modelId,"_",finaldf2$modelYear)

#finaldf2<-finaldf2[order(finaldf2$modelId, finaldf2$benchmarkPriority),]

#####Get the summary by modelId and benchmarkLevel and add how a column for model year count
summ<-group_by(finaldf2,modelId, benchmarkLevel)
summ<-summarise(summ,
                count=length(modelYear))
summ$benchmarkPriority<-NA
summ$benchmarkPriority[summ$benchmarkLevel=="sizeClass"]<-1
summ$benchmarkPriority[summ$benchmarkLevel=="subtype"]<-2
summ$benchmarkPriority[summ$benchmarkLevel=="category"]<-3

summ<-summ[order(-summ$count,summ$benchmarkPriority),]
#summ$id_priority<-paste0(summ$modelId,"_",summ$count)
summ2<-summ[!duplicated(summ$modelId),]
summ2$id_priority<-paste0(summ2$modelId,"_",summ2$benchmarkPriority)

finaldf2$id_priority<-paste0(finaldf2$modelId,"_",finaldf2$benchmarkPriority)

##### Merge summary (modelYear count) table with finaldf3
finaldf3<-merge(finaldf2,summ2, by="id_priority")

#finaldf3<-finaldf2[!duplicated(finaldf2$idyear),]
#finaldf3<-finaldf2[!duplicated(finaldf2$modelId),]

#####Calculate number of modelIds that fall under each number of modelYear count
ct<-data.frame(table(finaldf3$count))
names(ct)<-c("year_count","Freq")
ct$modelIdcount<-round((ct$Freq/as.numeric(levels(ct$year_count))),digits=0)
ct$year_count[1:length(ct$year_count)][[2]]
#total<-data.frame(Total=c(0,sum(as.numeric(ct$Freq)),sum(ct$modelIdcount)))
as.numeric(levels(ct$year_count))
ct[18,]<-list(0,sum(as.numeric(ct$Freq)),sum(ct$modelIdcount))
ct$freqpercent<-paste0(round(ct$Freq/ct$Freq[18]*100,digits=2),"%")
ct$modelpercent<-paste0(round(ct$modelIdcount/ct$modelIdcount[18]*100,digits=2),"%")

finaldf4<-finaldf3[(finaldf3$count==30|finaldf3$count==29),]
#finaldf3b<-finaldf3[finaldf3$count==30,]
665201-119201
names(finaldf3)
#finaldf4<-finaldf3
#####Formatting for final output
finaldf4<-select(finaldf4, modelId.x, modelYear, age, benchmarkLevel.x, meanAnnualUsage,recordCount, percentile5, percentile10, percentile15, percentile20,
                 percentile25, percentile30, percentile35, percentile40, percentile45, percentile50, percentile55, percentile60, 
                 percentile65, percentile70, percentile75, percentile80, percentile85, percentile90, percentile95, percentile99,
                 distribution5, distribution10, distribution15, distribution20, distribution25, distribution30, distribution35, 
                 distribution40, distribution45, distribution50, distribution55, distribution60, distribution65, distribution70,
                 distribution75, distribution80, distribution85, distribution90, distribution95, distribution99)

names(finaldf4)[1]<-"modelId"
names(finaldf4)[4]<-"benchmarkLevel"

###Count the total records for each benchmarkLevel
ddply(finaldf4, ~benchmarkLevel, summarize, total=length(modelYear))


####Check for modelIds not in finaldf4
notin<-unique(val$modelId[!val$modelId %in% finaldf4$modelId])
notin_detail<-val[val$modelId %in% notin,]




###Write ouput###

fwrite(finaldf4, file="C:/Users/LeeB/Documents/Usage Benchmarking/UsageBenchmarking_output_042419_3.csv", row.names=F)

table(finaldf4$benchmarkLevel)
























test1<-filter(finaldf2, modelId==31756)

#Number of each benchmark level
nrow(finaldf2[finaldf2$benchmarkLevel=="model",])
nrow(finaldf2[finaldf2$benchmarkLevel=="sizeClass",])
nrow(finaldf2[finaldf2$benchmarkLevel=="subtype",])
nrow(finaldf2[finaldf2$benchmarkLevel=="category",])

#sizetest1
unique(raw$sizeClass[raw$modelId==15947])
#sizetest2
unique(raw$sizeClass[raw$modelId==16298])

#subtest1
unique(raw$subtype[raw$modelId==53740])
#subtest2
unique(raw$subtype[raw$modelId==64169])

#cattest1
unique(raw$category[raw$modelId==596])
#cattest2
unique(raw$category[raw$modelId==185733])

#Find categories with "Trucks"
finaldf3<-finaldf2
finaldf3<-data.frame(finaldf3$modelId)
names(finaldf3)[1]<-"modelId"
finaldf3$category<-all$category[match(finaldf3$modelId,all$modelId)]
fincat<-data.frame(unique(finaldf3$category))
names(fincat)[1]<-"category"
fincat$category[grep("Trucks",fincat$category)]




################################################
#######testing only###########################
################################################
###############################################


sctable2$sizeClassConcat<-NULL
subtable2$sizeClassConcat<-NULL
subtable2$subtypeConcat<-NULL
cattable2$sizeClassConcat<-NULL
cattable2$subtypeConcat<-NULL


##New Data cleaning

#modtable3<-modtable2
#sctable3<-filter(sctable2, !(modelId %in% modtable3$modelId))
#subtable3<-filter(subtable2, !(modelId %in% modtable3$modelId) & !(modelId %in% sctable3$modelId))
#cattable3<-filter(cattable2, !(modelId %in% modtable3$modelId) & !(modelId %in% sctable3$modelId) & !(modelId %in% subtable3$modelId))

#nrow(modtable3) + nrow(sctable3) + nrow(subtable3) + nrow(cattable3)

#Formatting the file






finaldf<-rbind(modtable2, sctable2, subtable2, cattable2)






#Assigning Priorities for Deduping

finaldf2$benchmarkPriority<-NA

table(finaldf2$benchmarkLevel)

finaldf2$benchmarkPriority[finaldf2$benchmarkLevel=="model"]<-1
finaldf2$benchmarkPriority[finaldf2$benchmarkLevel=="sizeClass"]<-2
finaldf2$benchmarkPriority[finaldf2$benchmarkLevel=="subtype"]<-3
finaldf2$benchmarkPriority[finaldf2$benchmarkLevel=="category"]<-4


modtable4<-subset(finaldf2, modtable$recordCount>50)
sctable4<-subset(finaldf2, sctable$recordCount>75)
subtable4<-subset(finaldf2, subtable$recordCount>100)
cattable4<-subset(finaldf2, cattable$recordCount>150)

finalmods<-filter(finaldf2, benchmarkLevel=="model")
finalsc<-filter(finaldf2, benchmarkLevel=="sizeClass")
finalsub<-filter(finaldf2, benchmarkLevel=="subtype")
finalcat<-filter(finaldf2, benchmarkLevel=="category")


#Filtered by recordCounts Already
finalmods2<-filter(finalmods, modelId %in% modtable4$modelId)
finalsc2<-filter(finalsc, modelId %in% sctable4$modelId)
finalsub2<-filter(finalsub, modelId %in% subtable4$modelId)
finalcat2<-filter(finalcat, modelId %in% cattable4$modelId)

modsrejects<-filter(finalmods, !(modelId %in% finalmods2$modelId))
modsrejects2<-select(modsrejects, modelId)





finaldf2<-finaldf2[order(finaldf2$modelId, finaldf2$benchmarkPriority),]

finaldf2<-finaldf2[!duplicated(finaldf2$modelId),]

####################################
#####Filtering Against CUrrent DB###
#####################################

library(plyr)
library(dplyr)
library(tidyr)
library(data.table)
library(RPostgreSQL)

###Connection to the EQW PostgresQL DB
pw <- {
  "No2f3vOcAQBQT/8dIZAs"
}

# loads the PostgreSQL driver
drv <- dbDriver("PostgreSQL")
# creates a connection to the postgres database
# note that "con" will be used later in each connection to the database
con <- dbConnect(drv, dbname = "eqw",
                 host = "dev-eqw-master.c0tj3cv5x7jc.us-east-1.rds.amazonaws.com", port = 5432,
                 user = "eqw_readonly", password = pw)

####################################


dbclassifications<-dbGetQuery(con, 'select distinct p1."classificationId" from staging."classifications" p1')
dbcategories<-dbGetQuery(con, 'select distinct p2."categoryId" from staging."categories" p2')
dbsubtypes<-dbGetQuery(con, 'select distinct p3."subtypeId" from staging."subtypes" p3')
dbsizeclasses<-dbGetQuery(con, 'select distinct p4."sizeClassId" from staging."sizes" p4')
dbmanufacturers<-dbGetQuery(con, 'select distinct p5."manufacturerId" from staging."manufacturers" p5')
dbmodels<-dbGetQuery(con, 'select distinct p6."modelId" from staging."models" p6')

dbmodelsdates<-dbGetQuery(con, 'select p7."modelId", p7."dateIntro", p7."dateDisco" from staging."models" p7')

dbmodelsdates<-select(dbmodelsdates, modelId, dateIntro, dateDisco)

finaldf3<-filter(finaldf2, modelId %in% dbmodels$modelId & recordCount>5)

finaldf4<-merge(finaldf3, dbmodelsdates, by.x="modelId", by.y="modelId")

write.csv(finaldf3, file="E:\\EQWAtlanta\\ARD\\Market Data\\UsageBenchmarking_071918_output2.csv", row.names=F)






