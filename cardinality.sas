/*		---------------------------------------------------------------			*\
|*		PROGRAM: 	CARDINALITY.SAS												*|
|*																				*|
|*		AUTHOR:  	MICHAEL QUINN MAGUIRE, MS									*|
|*																				*|
|*		EMAIL: 		MICHAELQMAGUIRE2@COP.UFL.EDU								*|
|*																				*|
|*		PURPOSE: 	(1) SEE NUMBER OF OBSERVATIONS PER VARIABLE.				*|
|*				 	(2) SEE NUMBER OF MISSING OBSERVATIONS PER VARIABLE.		*|
|*				 	(3) SEE HOW MANY UNIQUE LEVELS EXIST IN A GIVEN VARIABLE.	*|
|*				 	(4) SEE METADATA LEVEL INFORMATION 							*|
|*				 	(5) SEE PERCENTAGE OF MISSING CASES.						*|
|*				 	(6) SEE PERCENTAGE OF UNIQUE CASES.							*|
|*																				*|
|*		PARAMETERS:	(1) 'DSN' - DATASET YOU WANT TO EXAMINE.					*|
|*						ASSUMES TWO-LEVEL NAME (E.G., WORK.WANT, SASHELP.CARS)	*|
|*					(2)	'_VARS` - VARIABLES YOU WANT TO EXAMINE.				*|
|*						YOU CAN LEAVE THIS BLANK IF YOU WANT TO SEE ALL,		*|
|*						YOU CAN ENTER ONE VARIABLE, OR YOU CAN ENTER MULTIPLE.	*|	
|*																				*|
\*		---------------------------------------------------------------			*/

%macro cardinality (dsn, _vars);

/* ------------------------------------------------------------------------------------------------------------------------ */
/* Setting up temporary directories so that this doesn't overwrite or delete any of their datasets in their work directory	*/
/* ------------------------------------------------------------------------------------------------------------------------ */

%let x = %sysfunc(getoption(work)); /* Extract work directory no matter the computer. */

%put &x.; /* Just for verification purposes. */

options dlcreatedir; /* this creates the directory if it doesn't exist. */

libname xtmp "&x.\xtmp"; /* Normal libname statement that creates directory wherever work directory is. */

/* ------------------------------------------------------------------------------------------------------------------------ */
/* Begin processing cardinality report.																						*/
/* ------------------------------------------------------------------------------------------------------------------------ */

/* If no variables are specified in the _vars parameter, do the following. */

%if %length(&_vars.) = 0 %then %do;

proc sql noprint;
	select
				trim(name),
				quote(trim(name))
					into	:vars separated by " ",	/* Create macro variabe containing each variable in the given dataset. */
							:vars_q separated by ", " /* Create macro variable containing each variable in dataset quoted and separated by a comma. */
	from
				dictionary.columns
	where
				libname = "%scan(%upcase(&dsn.), 1)" 	and	/* getting libref from dsn macro variable. */
				memname = "%scan(%upcase(&dsn.), -1)"; 		/* getting dataset name from dsn macro variable. */
quit;

%end;

/* If only one parameter is specified, do the following */

%else %if %length(&_vars.) = 1 %then %do;
	%let vars = &_vars.;
	%let vars_q = %quote(&_vars.);
%end;

/* If more than one variable is specified, do the following */

%else %do;

data _null_;
	requested_vars = "&_vars."; /* Store requested variables in the PDV. */
	length vars_quoted $5000.;	/* Create a new variable with an arbitrariliy long length depending on variables requested. */
	do i = 1 to countw(requested_vars, " "); /* Do for each variable in the requested_vars variable. */
		vars_quoted = catx(",", vars_quoted, "'" || scan(requested_vars, i, " ") || "'"); /* Take the first variable, quote it, and add a comma at the end. Continue until there are no more variables. */
	end; /* End the do processing. */
	call symputx("vars", requested_vars); /* Store the "requested_vars" variable into a macro variable called "vars". */
	call symputx("vars_q", vars_quoted); /* Store the "vars_quoted" variable into a macro variable called "vars_q". */
run;

%end;

/* Do the following process for each variable stored in the macro variable 'vars'. */
/* The %do loop essentially counts each element in the macro variable 'vars' and stores the number in a macro variable named 'i'. */
/* In other words, the first variable will have an &i. value of 1, the second will have 2, etc. */

%do i = 1 %to %sysfunc(countw(&vars., " "));

/* This %LET statement creates a local macro variable that stores captures the name of the variable depending on the position in the %do loop. */
/* Local macro variables only exist during the duration of the macro */
/* voi stands for variable of interest. */

%let voi = %scan(&vars., &i., " ");

/* This exists solely to deal with variables that are 32 characters. It truncates the variable name to 30 characters */

%if %length(&voi.) > 32 %then %do;

	%let voit = %substr(&voi., 1, 30);

%end;

/* If it's not longer than 32 characters, just set it equal to the variable of interest. */

	%else %do;

		%let voit = &voi.;

	%end;

proc sort data = &dsn. (keep = &voi.)
	out = xtmp._c&voit.;
		by &voi.;
run;

/* Run a DATA step that retains the last observation in the dataset. */

data 		xtmp._w&voit. (keep = 	 variable_name /* Name of variable. */
									 n_levels /* Number of levels in dataset. */
									 n_cases /* Number of observations in dataset */
									 n_miss /* Number of missing values in dataset */
									 percent_unique /* Metric ranging from 0 to 1 representing uniqueness of variable. 0 = perfectly unique, 1 same value */
									 percent_miss /* Metric representing how many cases are missing relative to all the observations. */
						   );
	set 	xtmp._c&voit. 
			end = z; /* Setting sorted dataset above and creating temporary variable called 'z' that marks whether an observation is the last observation in a dataset. */
	by 		&voi.; /* Using by-group processing for incremental counters for the variable under selection. */
	retain 	x n_miss 0; /* Retaining values for incrementing by-group processing. Retaining is also a compile-time only statement so it is theoretically faster than setting a variable to zero. */

			variable_name = vname(&voi.); /* Keeping variable name at end. */

			n_cases = _N_; /* Represents number of rows. */

			if missing(&voi.) then n_miss + 1; /* Begins at zero. If a record is missing, it increments by 1. This value is retained until it changes again or until the step reaches the last observation. */

			if first.&voi. then n_levels + 1; /* Again - begins at zero. Increments by 1 if it reaches a new level. This value is retained until it changes again or until the step reaches the last observation. */

			percent_unique = n_levels / n_cases; /* Number of unique levels divided by the number of observations. 0 represents a unique field, 1 represents a completely uniform field. */

			percent_miss = n_miss / n_cases; /* Shows percentage of missing records in a dataset. */

			if z then output; /* Checks if it's the last observation in the dataset, and if it is, it outputs all the calculations done above. */

	label	variable_name 		= "Variable"
			n_cases 			= "Number of Observations"
			n_miss 				= "Number of Missing Observations"
			n_levels 			= "Number of Unique Levels/Categories"
			percent_unique 		= "Percent Unique"
			percent_miss 		= "Percent Missing"; /* Labeling them all so the output is nicer. */


	format	percent_miss 	percent8.2
			percent_unique	percent8.2; /* Formatting for nicer output. */
			
run;

%end; /* Ends %do loop. Returns to the top if there are more variables to process. If there are no more, then the %do loop ends. */

/* Combining all the datasets into one. */

data xtmp.cardinality_report;
	retain variable_name n_levels n_miss n_cases percent_unique percent_miss;
	set xtmp._w:;
run;

/* Extract metadata info and store it into its own dataset */

proc contents data = &dsn. 
	out = xtmp.metadata (keep = name length varnum type label format formatl formatd informat informl informd) noprint;
run;

/* Sort by name for merge */

proc sort data = xtmp.metadata;
	by name;
	where upcase(name) in (%upcase(&vars_q.)); /* This is where we have to use the quoted vars. */
run;

/* Sort by name for merge */

proc sort data = xtmp.cardinality_report;
	by variable_name;
run;

/* Doing this to include character/numeric on final output */

proc format;
	value n2l 
		1 = "Numeric"
		2 = "Character"
;

/* Merge it all together and add final formatting touches. */

data 		xtmp.cardinality_report_fnl 	(drop = format formatd formatl informat informd informl type);
	merge	xtmp.cardinality_report 		(in = a)
			xtmp.metadata 					(in = b 
								 	 		 rename = (name = variable_name)
											);
	by		variable_name;

			if not missing(format) then do;
				_formatx 	= catx(".", cats(format, formatl), formatd);
			end;

			if not missing(informat) then do;
				_informatx 	= catx(".", cats(informat, informl), informd);
			end;

			_type = put(type, n2l.);

	label	_formatx 	= "Format"
			_informatx 	= "Informat"
			_type		= "Variable Type";
			
run;

/* Ordering for consistency with dataset viewing */

proc sort data = xtmp.cardinality_report_fnl;
	by varnum;
run;

/* The actual report */

title "Cardinality Report for %sysfunc(upcase(&dsn.))";
footnote "Percent Unique = Number of Levels / Number of Observations. 0.00% = Not Unique, 100.0% = Completely Unique";
footnote2 "Percent Missing = Number of Missing Observations / Number of Observations";
proc print data = xtmp.cardinality_report_fnl label noobs;
	var variable_name label n_levels n_miss n_cases percent_unique percent_miss length varnum _formatx _informatx _type;
run;
footnote;

/* Deleting everything in the xtmp directory. */

proc datasets library = xtmp kill nolist;
run;
quit;

/* Clear the temporary directory */

libname xtmp clear;

%mend cardinality;
