/****************************************************************************************
*  SAS CAS Risk Test Library  
*  NAME:             bnkdemo1
*  SUPPORT:          jowjoh
*  CREATED:          06 JUN 2019
*  PURPOSE:          Comparison join of cubes which are themselves aggregate joins.
*  EXPECTED RESULT:  Simstat data sets of aggregate joins compare equal within 1e-7 with 
*                    HPRisk. Log and listing should compare equal.
*
****************************************************************************************/

%setuptemp(rdlib,rdlib);
%let rdenvPath = &temppath.&pathsep.;

%setuptemp(hpexp,hpexp);

%setuptemp(cubes,cubes);
%let cubeloc = &temppath.&pathsep.;

%let EnvName = bankdemo;
%let currentDate = '25AUG2014'd;

%gridinit();
%let gridpath = /local/data/risk;

%let nodes   = 10;
%let threads = 10;

/****************************************/
/* Set Macro variables for scaling      */
/****************************************/
%let numBooks = 2;                /*This is the number of subcubes you'll create for the aggregate join*/
%let numPositionsBook = 5000;     /*This is the number of positions per book*/
%let numStates = 261;             /*Includes current value- so 261 for today and past year. Scale this to use more nodes.*/
%let numDays = 2;                 /*How many days to compare side-by-side*/


/****************************************/
/* Initiate a new CAS Session           */
/****************************************/

/*%let sysparm=rel:vb025;*/
/*%cassetup(forcestart=YES);*/

libname ctt cas caslib="CasTestTmp";

/****************************************/
/* Create Risk Dimensions Environment   */
/****************************************/

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

	call streaminit(1337);

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


proc risk;
	environment new =RDLib.&EnvName 
	Label= "&EnvName Risk Environment"
	instid_length = 32;

	instdata All_Positions file= RDLib.InstData format=simple; 
	declare instvars=(
		region                  CHAR        16   VAR   LABEL = "Region",
		country                 CHAR        16   VAR   LABEL = "Country",
		city                    CHAR        16   VAR   LABEL = "City",
		%declareinst(&numStates)
	);

	marketdata CurrentMkt file = RDLib.CurrentData  type = current;
	marketdata Scenarios  file = RDLib.ScenarioData type = scenarios interval = weekday;

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

	sources Positions 
		All_Positions 
	;
	read sources=Positions out=Portfolio;

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


/* ============================================================================ */
/* =================================== HPR ==================================== */
/* ============================================================================ */

libname portlib "&rdenvPath./&EnvName./_RD_PORTS/portfolio";
%let name = bankdemo;

proc hpexport 
	expprojlib = hpexp
	project= BankDemo_Small 
	env = RDLib.&EnvName;
run;


%macro CreateBookHistory(bookIndex, dayIndex);
	%if &dayIndex = 1 %then %do;
		proc datasets nolist;
			copy out=work in = portlib;
		run;
		quit;
		data work.HPRAggregation;
			set work.HPRAggregation;
			InstID = compress(InstID||"_"||&bookIndex);
		run;
	%end;
	%else %do;
	/*Add and remove positions for a new day*/
		data work.HPRAggregation;
			set work.HPRAggregation end = EOF;
			call streaminit(1337 + &dayIndex.);
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
			call streaminit(1337 + &dayIndex.);
			array State_Number[&numStates];
			do i = 1 to &numStates-1;
				State_Number[i] = State_Number[i+1];
			end;
			State_Number[&numStates] = State_Number[&numStates-1] * exp((-1*(.05**2)/2) + .05*rannor(&bookIndex+1234));
			drop i;
		run;
	%end;

	data _null_;
	   rundate = &currentDate.;
	   do j = 1 to &dayIndex.;
		  rundate = rundate - 1;
		  do while (weekday(rundate) = 1 or weekday(rundate) = 7);
			 rundate = rundate - 1;
		  end;
	   end;
	   rd = put(rundate, date9.);
	   call symputx("rundate",rd);
	run;


	proc hprisk
		task = clean
		cube = "&cubeloc.&name._Book&bookIndex._Day&dayIndex"
		;
	run;
	quit;

	proc hprisk 
		expprojlib     = hpexp 
		task           = runproject
		portlib        = work
		gridpath       = "&gridpath.&name._Book&bookIndex._Day&dayIndex"
		cube           = "&cubeloc.&name._Book&bookIndex._Day&dayIndex"
		cubelabel      = "Book number &bookIndex Day &dayIndex"
		rundate        = "&rundate"d
		; 
		performance 
			nodes=&nodes 
			nthreads=&threads
		;
		crossclassvars   
			region 
			country
			city
			instid
		; 	
	run;
	quit;

%mend CreateBookHistory;

%macro CreateAllBooks(numBooks, numDays);
	%do bookInd = 1 %to &numBooks;
		%do dayInd = 1 %to &numDays;
			%CreateBookHistory(&bookInd,&dayInd);
		%end;
	%end;
%mend CreateAllBooks;

%CreateAllBooks(&numBooks,&numDays);

/*Create Join Cubes*/

%macro makeSubCubePaths(numBooks,dayIndex);
	%do bookIndex = 1 %to &numBooks;
		"&cubeloc.&name._Book&bookIndex._Day&dayIndex"
	%end;
%mend makeSubCubePaths;


%macro CreateDailyAggregate(numBooks,numDays);
	%do dayIndex = 1 %to &numDays;
		proc hprisk
			task = clean
         cube = "&cubeloc.&name._Day&dayIndex";
		run;
		quit;

		proc hprisk
			task = joincubes
			jointype = aggregate
			cube = "&cubeloc.&name._Day&dayIndex"
			subcubes = (%makeSubCubePaths(&numBooks,&dayIndex))
			;
		run;
		quit;


		proc hprisk
			task = query
			cube = "&cubeloc.&name._Day&dayIndex";
			query region country city instid;
			query;
			output simstat =work.hpr_simstatDay&dayIndex.;
		run;
		quit;
	%end;
%mend CreateDailyAggregate;

%CreateDailyAggregate(&numBooks,&numDays);

%macro makeSubCubePaths2(numDays);
	%do dayIndex = 1 %to &numDays;
		"&cubeloc.&name._Day&dayIndex"
	%end;
%mend makeSubCubePaths2;

%macro CreateSidebySide(numDays);
	proc hprisk
		task = clean
		cube = "&cubeloc.&name";
	run;
	quit;

	proc hprisk
		task = joincubes
		jointype = comparison
		cube = "&cubeloc.&name"
		subcubes = (%makeSubCubePaths2(&numDays))
		;
	run;
	quit;
%mend CreateSidebySide;

%CreateSidebySide(&numDays);

/* ============================================================================ */
/* =================================== CAS ==================================== */
/* ============================================================================ */

proc hpexport
	project = BankDemo_Small
	env = RDLib.&EnvName.;
	casexport envTable = ctt.bankdemo_rdenv methodsTable = ctt.methods;
run;


/* Load data into CAS */

proc casutil outcaslib = "CasTestTmp";
	load data=RDLib.CurrentData replace;
	load data=RDLib.ScenarioData replace;
	load data=RDLib.InstData replace;
run; quit;

data ctt.bankdemo_rdenv;
   set ctt.bankdemo_rdenv;
   if substr(tableref,1,5)="RDLIB" then tableref=tranwrd(tableref,"RDLIB","CasTestTmp");
run;


/***************************************/
/*  Create sub cubes                   */
/***************************************/

%macro CreateBookHistory(bookIndex, dayIndex);

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
		
		call streaminit(1337 + &dayIndex.);
		
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
		
		call streaminit(1337 + &dayIndex.);
		
		array State_Number[&numStates];
		do i = 1 to &numStates-1;
			State_Number[i] = State_Number[i+1];
		end;
		State_Number[&numStates] = State_Number[&numStates-1] * exp((-1*(.05**2)/2)
									+ .05*rannor(&bookIndex+1234));
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

proc casutil outcaslib = "CasTestTmp";
	load data = work.HPRAggregation replace;
run; quit;


proc cas;
	loadactionset "riskrun";
   
	parms = {
		envTable = { caslib = "CasTestTmp" name = "bankdemo_rdenv" }
		instrumentTable = { caslib = "CasTestTmp" name = "HPRAggregation" }
		envOut = { caslib = "CasTestTmp" name = "bankdemo_book&bookIndex._day&dayIndex." }
		valuesOut = { caslib = "CasTestTmp" name = "values_book&bookIndex._day&dayIndex." }
		simStatesTable 	={
			table 			={caslib="CasTestTmp", name="scenariodata"},
			interval		="WEEKDAY",
			horizons		={1},
			nDraws			=&numStates.,
			asOfDate		=&rundate.
		}
	};
   
	action riskRun.evaluatePortfolio / parms;
run; quit;

/* Print out some of the environment table for benching purposes */
/*proc sort data=ctt.bankdemo_book&bookIndex._day&dayIndex. out=work.env;*/
/*	by category subcategory name;*/
/*run;*/
/*data _null_; set work.env;*/
/*	put CATEGORY= SUBCATEGORY= NAME=;*/
/*	put ATTRIBUTES=;*/
/*	put 60*"-";*/
/*run;*/

%mend CreateBookHistory;

%macro CreateAllBooks(numBooks, numDays);
	%do bookInd = 1 %to &numBooks;
		%do dayInd = 1 %to &numDays;	 
			%CreateBookHistory(&bookInd,&dayInd); 
		%end;
	%end;
%mend CreateAllBooks;

%CreateAllBooks(&numBooks,&numDays);


/* Create daily aggregate join cubes */

/* Lists cube names for all books for a single day */
%macro makeSubCubePaths(numBooks,dayIndex);
	%do bookIndex = 1 %to &numBooks;
		{ caslib = "CasTestTmp", name = "bankdemo_book&bookIndex._day&dayIndex." }
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
				envOut = { caslib = "CasTestTmp", name = "bankdemo_day&dayIndex." }
			};        
			action riskRun.join / parms;
		run; quit;

		/* Print out some of the environment table for benching purposes */
/*		proc sort data=ctt.bankdemo_day&dayIndex. out=work.env;*/
/*			by category subcategory name;*/
/*		run;*/
/*		data _null_; set work.env;*/
/*			put CATEGORY= SUBCATEGORY= NAME=;*/
/*			put ATTRIBUTES=;*/
/*			put 60*"-";*/
/*		run;*/

		/* Query cube to get reasonable benchmark values, then rebuild */
		/* NOT YET IMPLEMENTED IN CAS RISK */

		proc cas;
			loadactionset "riskResults";
			parms = {
				type = "AGGREGATE"
				envTable = { caslib = "CasTestTmp", name = "bankdemo_day&dayIndex." }
				outputs = {
					{
						type = "STAT", 
						outTable = {caslib="CasTestTmp", name="simstat&dayIndex."}
					}
				}
				outvars = { DROP = {"Value"}}
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

		proc casutil outcaslib = "CasTestTmp";
			load data = work.BenchmarkVals replace;
		run; quit;

		proc cas;
			parms = {
				benchmarkData = { caslib = "CasTestTmp", name = "BenchmarkVals" }
				type = "AGGREGATE"
				subEnvTables = {
					%makeSubCubePaths(&numBooks., &dayIndex.)
				}
				envOut = { caslib = "CasTestTmp", name = "bankdemo_day&dayIndex." }
			};        
			action riskRun.join / parms;
		run; quit;

		*/

	%end;
%mend CreateDailyAggregate;

%CreateDailyAggregate(&numBooks,&numDays);


/* Create side-by-side join of daily aggregate cubes */

%macro makeSubCubePaths2(numDays);
	%do dayIndex = 1 %to &numDays;
		{ caslib = "CasTestTmp", name = "bankdemo_day&dayIndex" }
	%end;
%mend makeSubCubePaths2;

proc cas;
	parms = {
		type = "COMPARISON"
		subEnvTables = {
			%makeSubCubePaths2(&numDays.)
		}
		envOut = { caslib = "CasTestTmp", name = "bankdemo_cas" }
	};

	action riskRun.join / parms;
run; quit;

/* Print environment table values */
/*proc sort data=ctt.bankdemo_cas out=work.env;*/
/*	by category subcategory name;*/
/*run;*/
/*data _null_; set work.env;*/
/*	put CATEGORY= SUBCATEGORY= NAME=;*/
/*	put ATTRIBUTES=;*/
/*	put 60*"-";*/
/*run;*/


/* ============================================================================ */
/* ================================ Compare =================================== */
/* ============================================================================ */

/* Here, we compare queries for each of the daily aggregate joins */

%macro compareSimstats(numDays);
	%do i = 1 %to &numDays.;
	  
		data work.cas_simstatDay&i.; 
			set ctt.simstat&i.;
			drop currexp peakexp expexp eee epe eepe ComputedVaR outputVariable
				_horizon_ _horidx_ ResultName Q1 Q3 condel tailindex varevt nreps;
			rename nmiss=nmissing contvar=contributionvar contes=contributiones;   
		run;

		data work.hpr_simstatDay&i.;
			set work.hpr_simstatDay&i.;
			drop EarliestExpireDateTime EarliestAlertDateTime ResultName ResultNumber
				AnalysisNumber AnalysisPart BaseDate SimulationTime SimulationHorizon
				SimulationPart SimulationPartLabel nrep mtm exposure;
		run;

		proc sort data=work.cas_simstatDay&i.; by instid; run;
		proc sort data=work.hpr_simstatDay&i.; by instid; run;

		proc compare note
			base        = work.hpr_simstatDay&i.
			compare     = work.cas_simstatDay&i.
			criterion   = 1e-7
		;
		run;      
	%end;
%mend compareSimstats;

%compareSimstats(&numDays.);

/*cas sascas1 terminate;*/
/*proc casoperate shutdown; run; quit;*/

