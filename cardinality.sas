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
|*					(2)	'VARIABLES` - VARIABLES YOU WANT TO EXAMINE.			*|
|*						YOU CAN LEAVE THIS BLANK IF YOU WANT TO SEE ALL,		*|
|*						YOU CAN ENTER ONE VARIABLE, OR YOU CAN ENTER MULTIPLE.	*|	
|*																				*|
\*		---------------------------------------------------------------			*/

%macro cardinality (dsn, variables);

/* 	------------------------------------------------------------------- 	*/
/* 	Step 1: Setting up enivornment.											*/
/* 	Setting up libname that outputs results into temporary directory. 		*/
/* 	This is done so that I do not clear out someone's existing results		*/
/* 	with the PROC DATASETS command at the bottom.							*/
/* 	------------------------------------------------------------------- 	*/

%let _whereswork = %sysfunc(getoption(work)); /* Locate work directory. */

options dlcreatedir; /* If directory doesn't exist, create it. */

libname _cardrpt "&_whereswork.\_cardrpt"; /* Create temporary directory. */

/* 	-------------------------------------------------------	*/
/* 	Step 2: Allocating requested variables.					*/
/* 	Begin storing requested variables into macro variables.	*/
/* 	Three things can occur here:							*/
/*	(1) User requests no variables, so select all.			*/
/*	(2) User requests one variable, so keep that one.		*/
/*	(3) User requests multiple variables, so keep those.	*/
/* 	-------------------------------------------------------	*/

/* If no variables requested, store all of them into a macro variable. */

%if %length(&variables.) = 0 %then %do; 

	proc sql noprint;
		select
					name
						into :voi separated by " "
		from
					dictionary.columns
		where
					libname = "%scan(%upcase(&dsn.), 1)" and
					memname = "%scan(%upcase(&dsn.), -1)";
	quit;

%end;

/* If one variable requested, just store it with a %let statement */

%else %if %length(&variables.) = 1 %then %do;

	%let voi = &variables.;

%end;

/* If multiple variables are requested, store them into a macro variable with a comma separator. */

%else %do;

	data _null_;
		requested_vars = "&variables.";
		length vars_quoted $5000.;
		do i = 1 to countw(requested_vars, " ");
			vars_quoted = catx("," vars_quoted, "'" || trim(scan(requested_vars, i, " ")) || "'");
		end;
		call symputx("voi", requested_vars);
	run;

%end;

/* 	-------------------------------------------------------	*/
/* 	Step 3: Calculate measures on each variable.			*/
/* 	-------------------------------------------------------	*/

/* Do this from the first word to however many words there are separated by spaces, and store the number in a macro variable called 'i'. */

%do i = 1 %to %sysfunc(countw(&voi., " "));

/* Scan the macro variable, &voi., which contains all the variables, and select the i'th word depending on the location in the %do loop. */

%let var = %scan(&voi., &i., " ");

/* If a variable name is longer or equal to 31 characters, trim it so the dataset will not exceed 32 characters. */

%if %length(&var.) >= 31 %then %do; 

	/* This is the truncated variable name, aka 'vart' for var trimmed. */

	%let vart = %substr(&voi., 1, 30); 

%end; 

/* If a variable name is less  */

	%else %do;
		
		/* Just set var trimmed equal to var if it's not longer than 31 characters. */
		
		%let vart = &var.;

	%end;

	/* Create a separate dataset for each variable with each measure.	*/

	proc sql;

		create table 	_cardrpt.c_&vart. as
			select
						"&var." 							as variable
																label  = "Variable Name"
						,
						count(&var.) 						as non_missing_obs
																label  = "Number of Non-Missing Observations"
																format = comma16.		
						,		
						count(distinct &var.) 				as distinct_obs
																label  = "Number of Levels"
																format = comma16.		
						,
						sum(
							case when missing(&var.)
							then 1
							else 0
							end
						)									as missing_obs
																label  = "Number of Missing Observations"
																format = comma16.		
						,
						calculated missing_obs  / count(*) 	as percent_missing
																label  = "Percent Missing"
																format = percent8.2		
						,
						calculated distinct_obs / count(*)	as percent_unique
																label  = "Percent Unique"
																format = percent8.2		
			from
						&dsn.;

	quit;

	/* Extract the metadata from the dictionary tables for the given variables. */

	proc sql;

		create table	_cardrpt.m_&vart. as
			select
						name
							label = "Variable Name",
						case 
							when type = "char"
								then "Character"
							when "num"
								then "Numeric"
							else "No Type"
						end as type_f
							label = "Variable Type",
						length
							label = "Variable Length",
						varnum
							label = "Position in Dataset",
						label
							label = "Variable Label",
						format
							label = "Variable Format",
						informat
							label = "Variable Informat"
			from
						dictionary.columns
			where
						libname 		= "%scan(%upcase(&dsn.), 1)" 	and
						memname 		= "%scan(%upcase(&dsn.), -1)" 	and
						propcase(name)	= "%sysfunc(propcase(&var.))";

	quit;

%end;

/* 	-------------------------------------------------------	*/
/* 	Step 4: Combining Datsets.								*/
/* 	-------------------------------------------------------	*/

/* Concatenate all the measure-level datasets into one. */

data _cardrpt.counts_all;
	length variable $32.;
	set _cardrpt.c_:;
run;

/* Concatenate all the metadata-level datasets into one. */

data _cardrpt.meta_all;
	length name $32.;
	set _cardrpt.m_:;
run;

/* 	-------------------------------------------------------	*/
/* 	Step 5: Create the final report.						*/
/* 	-------------------------------------------------------	*/

proc sql;
	create table	_cardrpt.cardinality as
		select		
					t1.*,
					t2.type_f,
					t2.length,
					t2.varnum,
					t2.label,
					t2.format,
					t2.informat
		from
					_cardrpt.counts_all as t1
						left join
					_cardrpt.meta_all as t2
							on	t1.variable = t2.name
		order by
					varnum;
quit;	
			
/* Print out the report. */
	
proc print data = _cardrpt.cardinality noobs label;
run;

/* Remove all the datasets from the _cardrpt libname */

proc datasets library = _cardrpt kill nolist;
run;
quit;

/* Clear the libname so it no longer exists. */

libname _cardrpt clear;

%mend cardinality;
