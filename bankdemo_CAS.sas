/*******************************************/
/* name:      bankdemo_CAS.sas
/* support:   jowjoh
/* details:
/*       This is a scalable version of the
/*       bank demo proof of concept. Use the
/*       macro variables below to scale 
/*******************************************/

/* =========== Table of Contents ============
*  PT1: Setup
*  PT2: Create Risk Dimensions Environment
*  PT3: Create Sub Cubes
*  PT4: Create daily aggregate joins
*  PT5: Create side-by-side joins
*  PT6: Promote and register cube
*  PT7: Clean up
* ==========================================*/

/* ============================================================================ */
/* =============================== PT1: Setup ================================= */
/* ============================================================================ */

/* First, we need a SAS lib for the risk dimensions environment */

	/*option dlcreatedir;*/
	/*%let rdenvPath = U:/temp/rdlib;*/
	/*libname rdlib "&rdenvPath.";*/

/* Or, you can use setuptemp if you're on SDSSAS */

	%setuptemp(rdlib,rdlib);
	%let rdenvPath = &temppath.&pathsep.;

/****************************************/
/*   Set Macro variables for scaling    */
/****************************************/

%let numBooks = 3;            /*Number of subcubes you'll create for the aggregate join*/
%let numPositionsBook = 1e5;  /*Number of positions per book*/
%let numStates = 261;         /*Number of states - includes basecase value*/
%let numDays = 3;             /*How many days to compare side-by-side*/

%let EnvName = bankdemo;
%let currentDate = '25AUG2014'd;


/****************************************/
/*     Initiate a new CAS Session       */
/****************************************/

/* Option 1: Connect to your own CAS Server running from command line or on a VM 
   This option is necessary if you want to open CAS cubes in the Risk Explorer. The
   environment promotion and registration will not work unless the CAS tables are
   written to a global caslib. */

	/*	%let user1 = jowjoh; */
	/*	%let port1 = 22626; */
	/*	%let host1 = "rskgrd41.unx.sas.com";*/
	/*	*/
	/*	%let port2 = 5570;*/
	/*	%let host2 = "v64-daappl-01.rqs-cloud.sashq-d.openstack.sas.com";*/
	/*	 */
	/*	options  */
	/*	   casuser=&user1 */
	/*	   cashost=&host1 */
	/*	   casport=&port1 */
	/*	 ; */
	/*	*/
	/*	cas sascas1;*/

	/* %let caslib = CASDisk; */
	/* %let registerFlag = T; */
	/* %let midtierServer = v64-daappl-01.rqs-cloud.sashq-d.openstack.sas.com; */
	/* %let workGroup = RQSGroupA; */

	/* caslib &caslib. */
	/*   datasource=(srctype="path") */
	/*   path="/home/jowjoh/casdisk" */
	/*   desc="CASDisk" */
	/*   global; */
	/* libname ctt cas caslib="&caslib."; */
	

/* Option 2: (SDSSAS) use cassetup to start a server on RDCGrid, launch a session
   called 'sascas1', and create a CAS lib called 'CASTestTmp'. Use this option if 
   you do not need to persist the environment after the CAS session is terminated */

	%let sysparm=rel:vb025;
	%let caslib = CASTestTmp;
	%cassetup(forcestart=YES);
	libname ctt cas caslib="&caslib.";
	%let registerFlag = F;


/* ============================================================================ */
/* ================ PT2: Create Risk Dimensions Environment =================== */
/* ============================================================================ */


/*Set up location hierarchy*/
data RDLib.LocationHashTable;
infile datalines delimiter = "|";
length region country city $16. LocationInd 3.;
input region country city $ LocationInd;
datalines;
Americas|Canada|Toronto|1
Americas|Canada|Montreal|2
Americas|Canada|Vancouver|3
Americas|Canada|Calgary|4
Americas|United States|New York City|5
Americas|United States|San Francisco|6
Americas|United States|Charlotte|7
Americas|United States|Dallas|8
Americas|United States|Boston|9
Americas|United States|Washington D.C.|10
Americas|United States|Chicago|11
Americas|United States|San Diego|12
Europe|United Kingdom|London|13
Europe|United Kingdom|Edinburgh|14
Europe|Germany|Munich|15
Europe|Germany|Frankfurt|16
Europe|Switzerland|Geneva|17
Europe|Switzerland|Zurich|18
Europe|Sweden|Stockholm|19
Asia|Australia|Sydney|20
Asia|Japan|Tokyo|21
Asia|China|Shanghai|22
Asia|China|Shenzhen|23
Asia|China|Hong Kong|24
Asia|Singapore|Singapore|25
;

/*Create instrument data*/
data RDLib.InstData;
length instid insttype $16. ;
length region country city $16.;
array State_Number[&numStates];
length currency $3. holding 3.;

if _n_ eq 1 then do;
	declare hash Location(dataset: "RDLib.LocationHashTable");
	rc = Location.definekey('LocationInd');
	rc = Location.definedata(all: 'yes');
	rc = Location.definedone();
end;

insttype = "HPRAggregation";
currency = "EUR";
holding = 1;


do i = 1 to &numPositionsBook;
instid = compress("InstID"||i);
LocationInd = rand('TABLE',
	.01, /*1*/
	.01,
	.01,
	.01,
	.20, /*New York*/
	.05,
	.04,
	.01,
	.01,
	.01, /*10*/
	.01,
	.01,
	.20, /*London*/
	.01,
	.05, /*Munich*/
	.05, /*Frankfurt*/
	.01,
	.05,
	.01,
	.01, /*20*/
	.10, /*Tokyo*/
	.01,
	.01,
	.10, /*HK*/
	.01
	)
	;
	rc = Location.find();
	State_Number[1] = 100*rand('Table',.3,.3,.4)* exp((-1*(.05**2)/2) + .05*rannor(12345));
	do j = 2 to &numStates;
		State_Number[j] = State_Number[j-1] * exp((-1*(.05**2)/2) + .05*rannor(12345));
	end;
	output;
end;
drop i j LocationInd rc;
run;

/*Create current market data*/
data RDLib.CurrentData;
StateIndex = &numStates;
run;

/*Create scenarios market data*/
data RDLib.scenariodata;
	length _horidx_ _rep_ StateIndex 8;
	_horidx_ = 1;
	do _rep_ = 1 to &numStates.;
		StateIndex = _rep_;
		output;
	end;
run;

%macro declareinst(numStates);
%do i = 1 %to &numStates-1;
   State_Number&i num var,
%end;
State_Number&i num var
%mend;

%macro instname(numStates);
%do i = 1 %to &numStates;
	State_Number&i 
%end;
%mend;



/* Load data into CAS */

proc casutil outcaslib = "&caslib.";
	load data=RDLib.CurrentData replace;
	load data=RDLib.ScenarioData replace;
	load data=RDLib.InstData replace;
run; quit;



proc risk;
environment new =RDLib.&EnvName  Label= "&EnvName Risk Environment"
instid_length = 32;
environment save;
run;

proc risk;
environment open = RDLib.&EnvName;

instdata All_Positions file= ctt.InstData format=simple; 
declare instvars=(
	region                  CHAR        16   VAR   LABEL = "Region",
	country                 CHAR        16   VAR   LABEL = "Country",
	city                    CHAR        16   VAR   LABEL = "City",
	%declareinst(&numStates)
);

marketdata CurrentMkt file = ctt.CurrentData  type = current;
marketdata Scenarios  file = ctt.ScenarioData type = scenarios interval = weekday;

declare riskfactor= ( 
	StateIndex num var 
);

declare outvars = (
   Larry       num      computed,
   Curly       num      computed,
   Moe         num      computed
);

environment save;
run;

proc compile  outlib= RDLib.&EnvName
              env= RDLib.&EnvName
              package= All_Functions;

method HPRAggregation kind=price;
   array State_Number[&numStates];
   _value_ = State_Number[StateIndex];
   Larry = _value_ + 100*rand('uniform');
   Curly = _value_ + 100*rand('normal');
   Moe = _value_ + 100*rand('erlang',3);
endmethod;

/*just a copy of VaR to check computed columns*/
method ComputedVaR kind = computed;
   ComputedVaR= get_queryval('_value_','VaR');
endmethod;

run;

proc risk;
environment open = RDLib.&EnvName;
instrument HPRAggregation
variables=(
	holding 
	region
	country
	city
	%instname(&numStates)
)
methods = (price HPRAggregation)
;
environment save;
run;

proc risk;
environment open=RDLib.&EnvName;
sources Positions 
	All_Positions 
;
read sources=Positions out=Portfolio;
environment save;
run;

proc risk;
environment open = RDLib.&EnvName;
crossclass LocationCC (
	region country city
);

simulation ScenarioSim
	method   =  scenario 
	data     =  Scenarios 
	interval =  weekday 
	horizon  =  (1) /*The number of horizons must be 1 for this example*/
	;

	project BankDemo_Small
		data            = (CurrentMkt)
		computedmethods = (ComputedVaR)
		portfolio   = Portfolio
		analysis    = (ScenarioSim)
		crossclass  = LocationCC
		Numeraire   = EUR
		options     = (SIMSTAT)
		outlib      = work
        ;

	environment save;
run;


/****************************************/
/*          Export to CAS               */
/****************************************/

proc hpexport
	project = BankDemo_Small
	env = RDLib.&EnvName.;
	casexport envTable = ctt.bankdemo_rdenv methodsTable = ctt.methods;
run; 


/* ============================================================================ */
/* ========================== PT3: Create Sub Cubes =========================== */
/* ============================================================================ */


libname portlib "&rdenvPath./&EnvName./_RD_PORTS/portfolio";


/* Each call to CreateBookHistory will create modified portfolio data, then run pricing to
/* get a sub cube for the given book index and day index.*/
%macro CreateBookHistory(bookIndex, dayIndex);

/* If it's the first day, copy generic portfolio from RD environment */
%if &dayIndex = 1 %then %do;
   proc datasets nolist;
      copy out=work in = portlib;
   run; quit;

   data work.HPRAggregation;
      set work.HPRAggregation;
      InstID = compress(InstID||"_"||&bookIndex);
   run;
%end;
%else %do;
   /*Add and remove positions for a new day*/
   data work.HPRAggregation;
      set work.HPRAggregation end = EOF;
	  call streaminit(1337 + &dayIndex. + &bookIndex.);
      if rand('Uniform') > .05 then output;
      if EOF then do;
         do i = 1 to ceil(.08*_n_);
            instid = compress("InstID"||_n_+i||"_"||&bookIndex);
            output;
         end;
      end;
      drop i EOF;
   run;

   /*Shift historical values and add new day value*/
   data work.HPRAggregation;
      set work.HPRAggregation;
	  call streaminit(1337 + &dayIndex. + &bookIndex. + 1);
      array State_Number[&numStates];
      do i = 1 to &numStates-1;
         State_Number[i] = State_Number[i+1];
      end;
      State_Number[&numStates] = State_Number[&numStates-1] * exp((-1*(.05**2)/2)
                                 + .05*rand('Normal'));
      drop i;
   run;
%end;

/* Get rundate based on day index */
data _null_;
   rundate = &currentDate.;
   do j = 1 to &dayIndex.;
      rundate = rundate - 1;
      do while (weekday(rundate) = 1 or weekday(rundate) = 7);
         rundate = rundate - 1;
      end;
   end;
   call symputx("rundate",rundate);
run;

/* Set staleness by rundate */
/* STALENESS NOT YET IMPLEMENTED IN CAS RISK */

/*data _null_;*/
/*   cubegroup  = mod(&dayIndex,3);*/

   /* Oldest cube is always red */
/*   if cubegroup eq 1 then do;*/
/*	   dt_red = intnx('weekday', rundate, &dayIndex ); */
/*	   dt_yellow = intnx('weekday', rundate, &dayIndex -2); */
/*   end;*/

   /* Second cube is always yellow */
/*   if cubegroup eq 2 then do;*/
/*      dt_red = intnx('month', rundate, 36); */
/*      dt_red = intnx('weekday', dt_red, -1* &dayIndex); */
/*      dt_yellow = intnx('weekday', rundate, &dayIndex - 2); */
/*   end;*/

   /* Third and more recent cubes are always green */
/*   if cubegroup eq 0 then do;*/
/*      dt_red = intnx('month', rundate, 36); */
/*      dt_red = intnx('weekday', dt_red, -1* &dayIndex); */
/*      dt_yellow = intnx('month', rundate, 36); */
/*      dt_yellow = intnx('weekday', dt_yellow, -1* &dayIndex); */
/*   end;*/
   
   /*Set Red*/
/*   staleness2 = dhms(dt_red, 0, 0, 0);*/
   /*Never turn yellow*/
/*   staleness1 = dhms(dt_yellow, 0, 0, 0); */

/*   t = put(staleness2,datetime.);*/
/*   t2 = put(staleness1,datetime.);*/
/*   t3 = put(rundate,date9.);*/
/**/
/*   call symput("cubetime", t);*/
/*   call symput("alerttime", t2);*/
/**/
/*run;*/


/* Load new portfolio data into CAS */
proc casutil outcaslib = "&caslib.";
   load data = work.HPRAggregation replace;
run; quit;

/* Run evaluatePortfolio to price and build the sub cube */
proc cas;
   loadactionset "riskrun";
   
   parms = {
      envTable = { caslib = "&caslib." name = "bankdemo_rdenv" }
      instrumentTable = { caslib = "&caslib." name = "HPRAggregation" }
      envOut = { caslib = "&caslib." name = "bankdemo_book&bookIndex._day&dayIndex." }
      valuesOut = { caslib = "&caslib." name = "values_book&bookIndex._day&dayIndex." }
	  simStatesTable 	={
			table 			={caslib="&caslib.", name="scenariodata"},
			interval		="WEEKDAY",
			horizons		={1},
			nDraws			=&numStates.,
			asOfDate		=&rundate.
	  }
   };
   
   action riskRun.evaluatePortfolio / parms;
run; quit;

%mend CreateBookHistory;

/* CreateAllBooks loops through the day and book indexes to create all sub cubes */
%macro CreateAllBooks(numBooks, numDays);

   %do bookInd = 1 %to &numBooks;
      %do dayInd = 1 %to &numDays;	 
         %CreateBookHistory(&bookInd,&dayInd); 
      %end;
   %end;

%mend CreateAllBooks;

%CreateAllBooks(&numBooks,&numDays);


/* ============================================================================ */
/* =================== PT4: Create daily aggregate joins ====================== */
/* ============================================================================ */


/* Lists cube names for all books for a single day */
%macro makeSubCubePaths(numBooks,dayIndex);
   %do bookIndex = 1 %to &numBooks;
      { caslib = "&caslib.", name = "bankdemo_book&bookIndex._day&dayIndex." }
   %end;
%mend makeSubCubePaths;

/* Aggregate-joins all cubes for a given day */
%macro CreateDailyAggregate(numBooks,numDays);
   %do dayIndex = 1 %to &numDays;

      proc cas;
         parms = {
            type = "AGGREGATE"
            subEnvTables = {
               %makeSubCubePaths(&numBooks., &dayIndex.)
            }
            envOut = { caslib = "&caslib.", name = "bankdemo_day&dayIndex." }
         };        
         action riskRun.join / parms;
      run; quit;

      /* Query cube to get reasonable benchmark values, then rebuild */
      /* NOT YET IMPLEMENTED IN CAS RISK */
      
      proc cas;
         loadactionset "riskResults";
         parms = {
            type = "AGGREGATE"
            envTable = { caslib = "&caslib.", name = "bankdemo_day&dayIndex." }
            outputs = { type = "STAT", outTable = "simstat&dayIndex." }
            requests = {
               { levels = { "region" "country" "city" "instid" } }
               { levels = {} }
            }
         };
         action riskResults.query / parms;
      run; quit;
      
      /*
      
      data work.BenchmarkVals;
         set ctt.simstat;
         ComparisonVaR1 = var * .90;
         ComparisonVaR2 = var * 1.10;
         keep region country city instid ComparisonVaR1 ComparisonVaR2;
      run;
      
      proc casutil outcaslib = "&caslib.";
         load data = work.BenchmarkVals replace;
      run; quit;

      proc cas;
         parms = {
            benchmarkData = { caslib = "&caslib.", name = "BenchmarkVals" }
            type = "AGGREGATE"
            subEnvTables = {
               %makeSubCubePaths(&numBooks., &dayIndex.)
            }
            envOut = { caslib = "&caslib.", name = "bankdemo_day&dayIndex." }
         };        
         action riskRun.join / parms;
      run; quit;

      */

   %end;
%mend CreateDailyAggregate;

%CreateDailyAggregate(&numBooks,&numDays);


/* ============================================================================ */
/* ===================== PT5: Create side-by-side joins ======================= */
/* ============================================================================ */

%macro makeSubCubePaths2(numDays);
   %do dayIndex = 1 %to &numDays;
      { caslib = "&caslib.", name = "bankdemo_day&dayIndex" }
   %end;
%mend makeSubCubePaths2;

proc cas;
   parms = {
      type = "COMPARISON"
      subEnvTables = {
         %makeSubCubePaths2(&numDays.)
      }
      envOut = { caslib = "&caslib.", name = "bankdemo_cas" }
   };

   action riskRun.join / parms;
run; quit;


/* ============================================================================ */
/* ===================== PT6: Promote and register cube ======================= */
/* ============================================================================ */

%if &registerFlag. = T %then %do;

	proc cas;
	   session sascas1;
	   parms = {
	      envTable = { caslib = "&caslib.", name = "bankdemo_cas" }
		  types={"ALL"}
		  name = "bankdemo_cas" 
	   };
	   action riskrun.promoteenv / parms;
	run; quit;

	proc cas;
	   session sascas1;
	   action riskRun.registerEnv /
	      envName="bankdemo_cas.rskenv"
	      envLabel="Bankdemo CAS risk environment"
	      workgroupName="&workGroup."
	      application = "hprisksvrc"
	      folderPath="/"

	      caslib="CASDisk"
	      envTableName="bankdemo_cas"

	      midtierServer="&midtierServer."
	      cubeName="bankdemo_cas"
	      operation="REGISTER"
	      ;
	   run;
	quit;

%end;
/* ============================================================================ */
/* ============================== PT7: Clean up =============================== */
/* ============================================================================ */

/*cas sascas1 terminate;*/
/*proc casoperate shutdown; run; quit;*/

