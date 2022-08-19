startTime <- Sys.time()
#############
# Libraries #
#############
library("optparse")
library("sqldf")

############
# Parameters
############

# Define possible parameters and default values
option_list = list(
make_option(c("-i", "--input"), type="character", default="test_data", help="Input directory", metavar="character"),
make_option(c("-w", "--work"), type="character", default="test_data", help="Working directory", metavar="character"),
make_option(c("-f", "--final"), type="character", default="test_data", help="Final directory", metavar="character"),
make_option(c("-d", "--includeDayOfWeek"), type="logical", default=TRUE, help="Output contain day of week", metavar="logical"),
make_option(c("-v", "--variable"), type="character", default="cost", help="Independent Variable", metavar="character"),
make_option(c("-k", "--kpi"), type="character", default="", help="Picked KPI", metavar="character")
)

opt_parser = OptionParser(option_list=option_list);
opt = parse_args(opt_parser);

# Assign parameters to variables
inputDirectory <- opt$input
workingDirectory <- opt$work
finalDirectory <- opt$final
generateDayOfWeekColumns <- opt$includeDayOfWeek
independentVar <- opt$variable
kpiVar <- opt$kpi

if (kpiVar == "") {
    stop("KPI cannot be empty")
}

cat("Input Directory: ", inputDirectory, "\n")
cat("Working Directory: ", workingDirectory, "\n")
cat("Final Directory: ", finalDirectory, "\n")
cat("Include Day Of Week: ", generateDayOfWeekColumns, "\n")
cat("Independent Variable: ", independentVar, "\n")
cat("Picked Kpi: ", kpiVar, "\n")


#############
# CONSTANTS #
#############
##### Define input file paths #####
kpiFilePath <- paste(inputDirectory, "/filteredkpi.csv", sep="")
mediaVehicleFilePath <- paste(inputDirectory, "/filteredmediavehicle.csv", sep="")
adInstanceFilePath <- paste(inputDirectory, "/filteredtvadinstance.csv", sep="")

##### Define output file paths #####
outputFilename <- paste(workingDirectory, "/media_transformed.csv", sep="")

#############
# SETUP
#############

setwd("./")
kpiMatrix <- read.csv(kpiFilePath)
tvadMatrix <- read.csv(adInstanceFilePath)
mediaVehicleMatrix <- read.csv(mediaVehicleFilePath)
# make sure the dates match in both input files since they are the thing you join on
tvadMatrix["Airing_Date"] <- as.Date(tvadMatrix$Airing_Date)
kpiMatrix$Date <- as.Date(kpiMatrix$Date)
mediaVehicleMatrix$Date <- as.Date(mediaVehicleMatrix$Date)

# Replace dots in column names with underscore for SQL

Airing_Cost_Temp <- apply(tvadMatrix, 1, function(row){
    if(as.numeric(row["Airing_Cost"]) == 0){
        actValue <- row["Airing_Estimated_Value"]
    } else {
        actValue <- row["Airing_Cost"]
    }
    return(actValue)
})
tvadMatrix <- cbind(tvadMatrix, Airing_Cost_Temp)

################
# STEP 1 - Aggregate TV Ad Costs(Spend) or Nielsen Viewers People 25-54 (Impressions)
################

print("STEP 1 - Aggregate TV Ad Costs")

if (independentVar == "impressions"){
  aggregateColumn <- "Impressions"
} else {
  aggregateColumn <- "Airing_Cost_Temp"
}

localCoverUpAggregate <- as.data.frame(sqldf(
  sprintf("SELECT Airing_Date, SUM(%s) AS 'Local_Cover_Up_Raw' FROM tvadMatrix  WHERE Local_Cover_Up GROUP BY Airing_Date", aggregateColumn)
))
names(localCoverUpAggregate)[names(localCoverUpAggregate) == 'Airing_Date'] <- 'Date'
names(localCoverUpAggregate)[names(localCoverUpAggregate) == 'Local_Cover_Up_Raw'] <- 'Local_Cover_Up_Raw'

programAggregate <- as.data.frame(sqldf(
  sprintf("SELECT Airing_Date, SUM(%s) AS 'Program' FROM tvadMatrix  WHERE Program_Analysis GROUP BY Airing_Date", aggregateColumn)
))
names(programAggregate)[names(programAggregate) == 'Airing_Date'] <- 'Date'
names(programAggregate)[names(programAggregate) == 'Program'] <- 'Program_Raw'

satelliteAggregate <- as.data.frame(sqldf(
  sprintf("SELECT Airing_Date, SUM(%s) AS 'Satellite_Variable' FROM tvadMatrix  WHERE Placement_Type = 'Satellite' AND Program_Analysis = 0 GROUP BY Airing_Date", aggregateColumn)
))
names(satelliteAggregate)[names(satelliteAggregate) == 'Airing_Date'] <- 'Date'
names(satelliteAggregate)[names(satelliteAggregate) == 'Satellite_Variable'] <- 'Satellite_Variable_Raw'

sqlString <- c("SELECT Airing_Date, SUM(%s) AS 'National' FROM tvadMatrix ",
                "WHERE Local_Cover_Up = 0 AND Program_Analysis = 0 ",
                  "AND Placement_Type != 'Satellite' AND Placement_Type != 'Broadcast - Local' ",
                  "AND Placement_Type != 'Cable - Local' AND Placement_Type != 'PI' ",
                  "AND Placement_Type != 'Streaming' ",
                  "AND Placement_Type != 'VOD' AND Placement_Type != 'Cinema' ",
                "GROUP BY Airing_Date")

nationalAggregate <- as.data.frame(sqldf(
  sprintf(paste(sqlString, collapse = " "), aggregateColumn)
))
names(nationalAggregate)[names(nationalAggregate) == 'Airing_Date'] <- 'Date'
names(nationalAggregate)[names(nationalAggregate) == 'National'] <- 'National_Raw'



#############
# STEP 2 - Add dayNum field
#############

print("STEP 2 - add daynum and dayNumIncrement fields")

# translate all Dates to a day of the week
dateRange <- data.frame(Date = kpiMatrix[, "Date"])
# wd <- weekdays(as.Date(dateRange[, "Date"]))
wd <- weekdays(as.Date(dateRange[, "Date"]))
# apply Weekday factor to give days number values
wdfactor <- factor(wd, levels = c("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"), ordered =TRUE)
# translate days of the week to their number equivlents
dayNum <- as.integer(wdfactor)
# add the daynum vector as a column to the joinedMatrix
# dateRange <- cbind(dateRange, dayNum)
minDate <- min(tvadMatrix[,"Airing_Date"])
dateRange <- data.frame(Date=dateRange[dateRange[,"Date"] >= minDate,])
dateRange <- cbind(dateRange, dayNumIncrement = rownames(dateRange))

#############
# STEPS 3 AND 4 - Create Days of the Week Table if necessary
#############

print("STEP 3 and 4: Creation of the days of the week columns if needed")

if(generateDayOfWeekColumns){
  for(i in 1:nrow(dateRange)){
    getWeekday <- weekdays(dateRange[i, "Date"])
    position <- as.integer(factor(getWeekday, levels = c("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"), ordered =TRUE))
    rowToInsert <- data.frame("Monday" = 0,"Tuesday" = 0,"Wednesday" = 0,"Thursday" = 0,"Friday" = 0,"Saturday" = 0,"Date" = dateRange[i, "Date"])
    if(position != 7){
      rowToInsert[position] <- 1
    }
    if(i == 1){
      dayOfWeekColumns <- data.frame("Monday" = rowToInsert[1], "Tuesday"= rowToInsert[2], "Wednesday"= rowToInsert[3], "Thursday"= rowToInsert[4], "Friday"= rowToInsert[5], "Saturday"= rowToInsert[6], "Date"= rowToInsert[7])
    } else {
      dayOfWeekColumns <- rbind(dayOfWeekColumns, i = rowToInsert)
    }
  }
  dateRange <- merge(dateRange,dayOfWeekColumns , by="Date", all=TRUE)
}

#############
# STEP 5 - Join Aggregate Ad Instance
#############
print("STEP 5 - Join Aggregate Ad Instance")
if (independentVar == "impressions"){
  mediaVehicleVariable <- "Impressions"
} else {
  mediaVehicleVariable <- "Spend"
}

outputDataFrame <- merge(dateRange, localCoverUpAggregate , by="Date", all=TRUE)
outputDataFrame <- merge(outputDataFrame, programAggregate , by="Date", all=TRUE)
outputDataFrame <- merge(outputDataFrame, satelliteAggregate , by="Date", all=TRUE)
outputDataFrame <- merge(outputDataFrame, nationalAggregate , by="Date", all=TRUE)

vodColumn <- data.frame(Date=mediaVehicleMatrix[,"Date"], VOD_Raw=mediaVehicleMatrix[sprintf("VOD_%s", mediaVehicleVariable)])
names(vodColumn)[names(vodColumn) == sprintf("VOD_%s", mediaVehicleVariable)] <- 'VOD_Raw'

ottColumn <- data.frame(Date=mediaVehicleMatrix[,"Date"],OTT_Raw=mediaVehicleMatrix[sprintf("OTT_%s", mediaVehicleVariable)])
names(ottColumn)[names(ottColumn) == sprintf("OTT_%s", mediaVehicleVariable)] <- 'OTT_Raw'

huluColumn <- data.frame(Date=mediaVehicleMatrix[,"Date"],Hulu_Raw=mediaVehicleMatrix[sprintf("Hulu_%s", mediaVehicleVariable)])
names(huluColumn)[names(huluColumn) == sprintf("Hulu_%s", mediaVehicleVariable)] <- 'Hulu_Raw'

otherTvColumn <- data.frame(Date=mediaVehicleMatrix[,"Date"],Other_TV_Raw=mediaVehicleMatrix[sprintf("Other_TV_%s", mediaVehicleVariable)])
names(otherTvColumn)[names(otherTvColumn) == sprintf("Other_TV_%s", mediaVehicleVariable)] <- 'Other_TV_Raw'

if(sum(vodColumn$VOD_Raw) > 0) outputDataFrame <- merge(outputDataFrame, vodColumn, by="Date", all=TRUE)
if(sum(ottColumn$OTT_Raw) > 0) outputDataFrame <- merge(outputDataFrame, ottColumn, by="Date", all=TRUE)
if(sum(huluColumn$Hulu_Raw) > 0) outputDataFrame <- merge(outputDataFrame, huluColumn, by="Date", all=TRUE)
if(sum(otherTvColumn$Other_TV_Raw) > 0) outputDataFrame <- merge(outputDataFrame, otherTvColumn, by="Date", all=TRUE)
outputDataFrame <- merge(outputDataFrame, data.frame(Date=kpiMatrix[,"Date"],KPI_Raw=kpiMatrix[,kpiVar]), by="Date", all=FALSE)
outputDataFrame[is.na(outputDataFrame)] <- 0
################
# STEP 6 - Run and add Log Calculations
################
print("STEP 6 - Run and add Log Calculations")
logColumns <- grep("_Raw", names(outputDataFrame), value = TRUE)
for(i in 1:length(logColumns)){
  colnameToLog <- logColumns[i]
  zzz <- 1
  colToLog <- apply(outputDataFrame, 1, function(inputRow){
    valueToLog <- as.numeric(inputRow[colnameToLog])
    logValue <- log(valueToLog + 0.0001)
    zzz <- zzz + 1
    return(logValue)
  })
  outputDataFrame <- cbind(outputDataFrame, colToLog)
  names(outputDataFrame)[names(outputDataFrame) == "colToLog"] <- sprintf("%s_Log", substr(colnameToLog, 1, nchar(colnameToLog)-4))
}

################
# STEP 7 - Calculate Mean centering + Variance
################
print("STEP 7 - Calculate Mean centering + Variance, but this should occur in regression")

################
# STEP - 8 - output
################
print("STEP 8 - write output")
write.csv(outputDataFrame, outputFilename)
endTime <- Sys.time()
runTime <- startTime - endTime

print(runTime)
