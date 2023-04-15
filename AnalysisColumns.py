import pandas as pd


class DataAnalysisColumns():

    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.columns = df.columns
        self.TotalRows = df.shape[0]
        self.BoxResult = []

        self.СolumnsDict = {
                  'AnalysisParams':'',
                  'NameColumns' : '',
                  'Value': ''}

    def CreateDf(self,BoxResult:list):
        
        OutputDf = pd.DataFrame(data=BoxResult, columns=list(self.СolumnsDict.keys()))
        self.BoxResult.clear()
        
        return OutputDf
    

    def AnalysisOfColumnNames(self):
        for NameColumn in self.columns:
            if " " in NameColumn:
                ListAttributes = ['Сolumn name', NameColumn, NameColumn]
                self.BoxResult.append(ListAttributes)
        
        return self.CreateDf(self.BoxResult)

    
    def AnalysisOfNull(self):

        for NameColumn in self.columns:
            CountNull = self.df[NameColumn].isnull().sum()

            if CountNull > 0:
                ListAttributes = ['Null Count', 
                               NameColumn , 
                               f'{CountNull} of {self.TotalRows} or {"{0:.0%}".format(CountNull/self.TotalRows)}']
            
                self.BoxResult.append(ListAttributes)
        
        return self.CreateDf(self.BoxResult)
                
 


    def AnalysisOfType(self): 

        for NameColumn in self.columns:
            TypeColumn = str(self.df[NameColumn].dtype)

            if TypeColumn == 'object':
                self.df['TypeData'] = self.df[NameColumn].apply(lambda x: str(type(x)))

                PivotTable = self.df.groupby('TypeData').agg({'TypeData': ['count']}).reset_index()
                PivotTable.columns = ['TypeData', 'count']
                ColumnType = PivotTable['TypeData']
                BoxResult = []

                for i in range(len(PivotTable)):
                    FirsttNumOfSymbol = ColumnType[i].find("'")
                    LatsNumOfSymbol = ColumnType[i].rfind("'") + 1
                    NameTypeRow = ColumnType[i][FirsttNumOfSymbol:LatsNumOfSymbol]

                    Percent = "{0:.0%}".format(PivotTable['count'][i]/self.TotalRows)

                    StringForAppend = f"{NameTypeRow}:{PivotTable['count'][i]}({Percent})"

                    BoxResult.append(StringForAppend)

                ListAttributes = ['Type', 
                                NameColumn, 
                                ",".join(BoxResult)]
                
                self.BoxResult.append(ListAttributes)

        return self.CreateDf(self.BoxResult)
                
    def AnalysisOfOutlier(self):
        for NameColumn in self.columns:
            TypeColumn = str(self.df[NameColumn].dtype)
            ColumnDf = self.df[NameColumn]


            if TypeColumn == 'float64' or TypeColumn == 'int64':

                IQR = (ColumnDf.quantile(.75) - ColumnDf.quantile(.25))
                UpperQuantile = ColumnDf.quantile(.75) + 1.5 * IQR
                LowerQuantile = ColumnDf.quantile(.25) - 1.5 * IQR

                RowsWithoutOutliers = len(ColumnDf[(ColumnDf < UpperQuantile) & (ColumnDf > LowerQuantile)])
                NumberOutliers = self.TotalRows - RowsWithoutOutliers

                PercentOutliers = "{0:.0%}".format(NumberOutliers/self.TotalRows)

                StringAppend = f"{self.TotalRows - RowsWithoutOutliers}({PercentOutliers})"

                ListAttributes = ['Outlier', 
                                NameColumn, 
                                StringAppend]
                
                self.BoxResult.append(ListAttributes)

        return self.CreateDf(self.BoxResult)
                
    def AnalysisOfUniqueText(self):
        for NameColumn in self.columns:

            ColumnDf = self.df[NameColumn]
            CountText = len(ColumnDf.unique())
            TextUni =  ColumnDf.unique() if CountText < 5 else ''

            StringAppend = f'{CountText} uniques {TextUni}'
            print(NameColumn)
            ListAttributes = ['Text', 
                        NameColumn, 
                        StringAppend]
            
            self.BoxResult.append(ListAttributes)

        return self.CreateDf(self.BoxResult)
    
    def AnalysisOfDf(self):
        ListOfAnalysis = [self.AnalysisOfColumnNames(),
                          self.AnalysisOfNull(),
                          self.AnalysisOfType(),
                          self.AnalysisOfOutlier(),
                          self.AnalysisOfUniqueText(),
                 ]
           
        OutputDf = pd.concat(ListOfAnalysis).reset_index(drop=True)

        OutputDf = OutputDf[['NameColumns', 'AnalysisParams', 'Value']]

        return OutputDf.sort_values(by=['NameColumns']).reset_index(drop=True)
        
