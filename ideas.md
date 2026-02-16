
#       A cunt ton ae ideas

##      What's great

The UI is beautiful.
It responds to user interaction in an intuitive manner.
The design elements are clear - both visually distinct from each other, and serving as good signposts for the user about aspects of pipeline development and execution.

I particularly like a lot of details that are missing from radar:
- The file has a nice tick beside it when it loads data in
- The blinking yellow light in the top-left when there's unsaved changes
    - This was less obvious to begin with, but is a useful indicator
- Execution status light on the component, although it works in a slightly unintuitive way (see below)

##      Issues / Not Great

- Data inputs has one input - I think it shouldn't have any data inputs.
- No restrictions on polars formula I can see - will elaborate later.
- When I try to join two data frames, the in-built predictor wants to do a key join
    - It's not possible to select the key column from the input component
- When I create a polars component with no code, the output schema has all column names
    - This even with two identical input data sources - same schema, all column names duplicated
    - Signals that schemas are dynamic and there's no control over variable-name collision
- Part of a wider problem - schemas don't allow to select columns to send or not; schemas accumulate as data is processed
    - Doubly an issue at output, where there may be a downstream data contract, or where calculation of full schema is costly and unnecessary if columns could be omitted
    - Unclear based on current architecture if performance gains for column selection are possible
- "External File" component isn't clear - maybe "Import Model/Container/Wrapper" or some synonym that encompasses other "picklable" transformation layers. Something like processing step.
- Output JSON adds fields based on selection order - not schema-table order. This makes it hard to configure. More intuitive to make fields reorderable in schema table and reflect that in JSON I think.

###     Data sink

Not sure what output does that a data sink doesn't - could wrap both in one like the inputs, with switchable API behaviour.

It would be useful to have an overall model live-rating configuration component like radar does.
This would - like radar - allow outputs to be switched off for live rating.
The component configurations should be clickable from this screen to allow for easy navigation.
Aside: forward/back buttons to aid navigation would be ace to include here.

This setting should be independent of API configuration, which should be shown on the config screen, and be a pre-requisite for turning on for live rating.
The actual configurations could either:
- map into a python (nested) dictionary (say via XPath or similar) which is mapped into the output API schema by a separate config component
- as above, but with possibly multiple dict-to-api mapping components downstream of the sinks, and one turned on depending on the environment/configuration
- configure the individual outputs indpenently.

I personally reckon the first or second option is better for live rating - first for simplicity and unified tesing approach, second for flexibility (including emulating testablity with a single all-sinks test interface).
Testing could then be done in python on the internal dictionary representation of the data, or on the JSON produced by the configured mapping to emulate the API return.

This map-from-dict approach could be a useful feature for input data too, again configurably.
Let's call these API sink/source components.

It also meshes well with data-contract validation. 
Since data contracts are usually parsable into a number of tables, given the canonical mapping of the data contract into a dictionary the tables are easy to contruct.
Wrapping this process in a wizard tool could automatically generate the entire IO interface for the model with a click.

Ditto, reading a request/response message into the API sink/source could generate the canonical input/output request-response message-to-dictionary mapping respectively.
The data source/sinks could then plug into the API sink/source and a table from the dictionary could be selected (optionally fields inherited from parent tables) to generate the sink/source's table schema.

I see this working on like to the setup button on a data/live sink/source in radar.
The big advantage I see is inheritance on the live side, and being able to set up the entire interface in one click.
Radar handles inheritance gracefully in execution, but doesn't do it well at configuration.
These operations are tedious, so big wins.

###     Undo

There's no undo - I think this is a must-have.
Could be just not working on mac, but Cmd-Z and Ctrl-Z did nothing after I made a change I wanted to roll back.

###     Execution status light - links vs everything-on data model

The execution status light on the component works in a slightly unintuitive way.
I think this is probably a defect in the data model implementation.

It seems to light up depending on execution status of potential upstream components independent of links.
So if you add a data source to a pipeline and run it, the execution status light will light up for most components on the canvas despite no drawn links to the data source.

The data model issue is definitely true for a polars component.
I can't confirm it's an issue for an output, except when linked from a polars component.
So it may just be a bug in the polars component.

This compromises both the UI and also potentially creates risks around execution where more than one primary key is in play in more complex pipelines.

###     Data ports

There's an output port on the data sink and output components, and an input port on the data source.
I think these shouldn't exist, just to simplify the UI and avoid any issues in the data model creating links from them.

I was also thinking more complex operations involving joins could have optional additional in-ports.
This would allow visual separation of data with different keys, and could tie in with separate objects in the data model to logically group and separate data sources. 
I think this is useful if using a more static approach to data schemas mid-flow, as it decouples the schema from the join logic.

##      Ideas

###     Asynchronous execution

Not clear to me if this is baked in, but it would be useful to have the pipeline run asynchronously.

I mean both inside the model, and also with respect to API calls.
Might be happening already - very compatible with polars.

API timeouts etc are not baked in though.
Non-critical API calls should have timeout cutoffs, for non-critical inputs such as credit rating factors etc.
Not sure whether to bake failover behaviour into the input for an API or to separate this out.

###     Primary Key

I think this should be defined in the data source component.

###     Data inputs

I like the structure of this component.
I really like that by default it has a browser for the file system starting at the project root and not navigable outside of that.

I think it would be great to have a URI bar above that with a browse button to navigate to a file anywhere on the system.
That could be hidden behind a drawer opener at the right, in-line with the `FILE` header
- one of those where it points left when closed, down when open
- expands with a smooth animation to expose the file address bar and browse button
- a drag-and-drop area for files between that and the project-root browser

This could take arguments for config from calls to the API the pipeline sits behind in live deployment.
Probably requires some additional options be available if API Input toggle is used.

I think it's worth having a window into online data sources like databricks or SQL Server.
It should load like the connection window in VSCode to allow tables to be selected from listed options.
A good idea is load the listing into a dataframe temporarily and allow it to be filtered or searched to ease user access.
The connection config could be entered as components in a connection string-type form, or inherited from a config component selected via dropdown.

Good to add test-connection functionality for online sources, and a small refresh control at the right of the table header for the TABLES section which would replace the FILE section when flat file input is selected.

###     Config input

Functionally this is a data input, but has slightly different behaviour and treats its data differently.
It is intended to control the logic of how the pipeline executes rather than providing data content.

It contains three kinds of data ingestion:
- From an API
- From a static file
- From a hard-coded set of values

Functions a lot like a switch in radar - differential behaviour could be triggered by logic involving:
- environmental variables
- a config file
- sequential failover logic like "look for file, then API"
- hard-coded values, possibly as optional last-resort failover behaviour
- more complex logic that I don't know how to define yet - possibly only via "code"

These could be set from drop-downs on the model-build canvas (say with a small ">>" element in a corner).
Emulates various environments (dev, test, prod) with different config inputs.

Could also be used to differentiate data sources for e.g. non-back/forward compatibility testing, such as change in data schema of inputs.

###      Control/config components

Don't know how well this aligns with your vision for the model.

A control/config component is a component that can be used to control model behaviour.
Similar to the control components in radar, where you could associate a data component with a control component.

The idea I have is a little bigger.
A control component would control a data component or a control component.

As an example, let's say we have two schemas - v1 and v2 - and want to do regression testing.
If we have identical data mapped in both schemas, a switch could be used to control which schema to use.
In reality this could mean any of the following:
- Switch chooses upstream data pipelines like in radar
- Switch passes differential parameter as control to config components, simultaneously changing APIs to use, source tables to query, file addresses to read etc
- Switch looks in different places to load in global configurations, e.g. API keys, model URIs, etc

Advanced behaviours could include:
- Sequential failover logic 
    - for all: if not all sources for a switch option are available then try the next option for all simultaneously
    - for each: if some source is available then try the next option - useful for e.g. data enrichment API downtime
- Hard-stop on particular failure modes
- Logging of failure or failover to a configurable destination for later analysis

Radar does this badly visually - a box floating in space with possibly no relationship to the components it controls.
I think our solution should be visual "control lines" that connect to the components they control.
These should be visually distinct from data lines, so some number of:
- different colour
- different termination direction or position - say down into the top of the component near left
- different shape of node at termination - say a square where data lines have small triangles pointed right
- different stroke - dotted (say) lines where data lines are solid
- different shape - drawn as orthogonal paths where data lines are smooth Bézier curves or cubic splines
- different arrowhead - none where data lines have arrows
- different connection property - say data lines connect to edge of node with no change, control lines connect to a hollow shape and it fills in

Like data schemas, component configurations should have "control schemas" in which the controls used to configure the component are chosen.
That way some behaviours can be hard-coded into the component and some can be configured at dynamically  at runtime, say from environmental variables or externally-defined configuration.

I think where data exists in the output data schema of a connected control, a data component's config fields should have a switch button to the left which highlights when definition is delegated to the control component.
I was thinking of a quarter-note rest music symbol.

###     Data Output Component

I like that the output schema is selectable here.
I think all comopnents outputting data internally in the pipeline should have this.

I think add a couple of elements to the schema table:
- A tickbox column header on the required tickbox to mass-select/unselect fields
- Key columns shown with a key icon
    - Selected by default if specified - need to figure out how to handle keys in underlying data model

###     Data contract validation

Set up callout to a data contract source - the best one I know is XSD so will user that as example.

An XSD essentially defines the schema of an XML document:
- what elements are allowed - types, names, sub-elements/children, number of occurrences
- what order the elements must be in
Similar things exist for JSON, but I'm not as familiar.

It's possible to point to a data contract and validate data against it on input/output.
This could also be used to define what fields to lift from input data to the input data frame, or vice-versa on output.

If a model's input or output is fully covered by data contracts, it could be used as a way to define the schema of the input data frame.
That is, a contract.py file's version history could be used as a version control mechanism for the model's data.

Using control, it could be possible to switch between different data formats for a model, for e.g. monitoring an existing predictive model on recent risks written on a changed stored quote format.

###     "Code"

I think most "code" should be in a notebook-like structure.
A possible exception is a "free-code" component.

It's good for prototyping if blocks can execute sequentially like in a normal notebook.
If we back it with polars and wrap it with config options choosing the calculated data-frame to output, and a selection of fields to pass to outputs, then it'll be efficient both deployed and in configuration.

I think fundamentally all steps should create output data frames - ideally 1 per step.
This is not completely true, as you might need to scan for outputs.

These should have a schema viewable on creation, and (partial) results viewable on (partial) execution.
I say partial, as some columns may be evaluated early by other operations.

Many familiar operations from radar could be implemented as code steps - filter, agg, join, agg by values, plus some additions like un/pivot and lots of column operations.
These all have the property that they change a data table in some way, producing a new data table.

These are perfect for a fluid interface in code, realisable as a block of operations presented sequentially in the GUI replacing a number of code blocks.
The configurations for such an operation sequence consists of per-operation configuration, plus a selection of data frame to use as output, and selection of fields thereof to form an output schema.

###     Polars Component

####    "Code"

I think all "code" should be in a notebook.

Cells could be hand-written or could be generated code using standard config options like radar operations.
This enforces standards around pipeline operations, allowing the code to be more queryable.
Also allows lower learning curve - GUI over code means less to learn initially.
Decoration on the functions (or attributes on classes) defining these canned operations could create useful metadata improving parsability of the model, including for querying and any operation where an AI model searches the codebase to assist the user.

####    Security - `literal_eval` and beyond

It is not possible to do all possible code executions we want with `literal_eval`.
It doesn't evaluate functions or methods, and can't work with anything that's not a literal whose type falls into a short list of the usual built-in suspects.

For polars, we would need to write something new.
I don't think it would be possible to write something "safe" that allows functions etc, due to monkey-patching.

Even ignoring this "vulnerability", it would be a lot of work to set up.
I say "vulnerability" because the user can already access the file system or online data sources, so it code execution doesn't really create a new attack vector.
It would also require some custom configuration options to allow the user to whitelist imported functions, class methods and objects that fall outside a typical use case we'd set up ourselves.
More discussion in an appendix section at the end - no point breaking the flow here.

####    Polars Output Schema - Static vs Dynamic

There's two dimensions to the output schema - namely availability and inclusion.
Availability means the output is calculated and will be available to downstream components.
Inclusion means the output is included in the schema table.

I think there's two obvious options for the output schema, as well as a non-obvious spectrum of mixes:
- Static - the output schema is defined as a table and all fields listed are expected to be calculable/in the data frame
    - Essentially, availability is ignored and inclusion must be specified
    - Radar uses this for most components; not necessarily good, but decouples the data model from the code
    - Disadvantage for situations where schema depends critically on configuration
        - This opens a can of worms I don't want to touch unless we have to - certainly kick this can down the road for now
- Dynamic - the output schema is set based on all available calculations in the component, and the schema table is generated dynamically every time the screen opens or code changes
    - This seems to be the current behaviour for all components; note all inputs are also outputs currently
    - Here downstream components are able to use all available fields - no inclusion specified

There are other mixed options:
- Input-dependent. Outputs follow availability as per current behaviour, but inputs are specified statically
    - Some radar components follow this model - joins, filters, aggregations. Generally could be inserted as prefab steps in a notebook though.
- 

The static schema is the most common and simple option.
A sensible mixed option is that:
- a static schema is set and each field here added to "required outputs" schema
- each field present in required outputs is validated: it must be created or exist in the inputs namespace
- a dynamic schema is calculated and available in the output namespace
- whenever a dynamic field is linked to another component, it would be added to the "required outputs" schema
- validations are required for all fields in "required outputs" just like static schema
- whenever a dynamic "required output" field is removed from all links, the "required outputs" schema removes it
This allows a mix of data standardisation using static schemas and flexibility using dynamic schemas.

Standard columns not defined could be set by options as creating a null value or failing loudly and preventing component execution downstream (per component, or per field).

####    Imports

I really love this feature.
I think a linter should scan it and warn about failed dependencies.

Could be useful to link to AI and get it to run a command with user approval like agents in vscode.

Additionally, this usually has "run" vs "skip" - what about "help"?
Open a new tab with a search that asks a trusted AI about the command.
Which AI should be configurable in user options.



#       Appendix
##      Unstructured ideas. Claude can help me structure them.
- An MCP sidebar that plugs into an MCP server we set up
- Logging to file, collected usage info to predict future usage and help train LLMs
- Data inputs should be able to be mixed - API or static file depending on context
- Config switches that hard-code things like API keys, model URIs, etc. 
    - I see this coming from a "config input" component

##      Security for Code Execution

Ideally code would run as a sandbox with:
- no write access to existing namespaces, the file system and objects in memory
- no access to objects in the existing namespaces
    - namespace is purely the dataframe input to the component, plus some whitelisted operations/functions
    - not even all functions, imports from a module or class methods available - only "safe" ones.

This could be configurable as a separate python process with either:
- no write access to the file system or any data sources
- access only to whitelisted "safe" functions
- code run by python in a new process only upon successful AST validation.

Such abstract syntax tree (AST) validation must include at least the following:
- very small footprint of code execution from imported packages
    - code validation would have to work on a "safe" whitelist model
        - some packages are known to have no side effects
        - some have functions or class methods with no side effects
    - this requires a small number of useful imported classes/methods/functions available
    - specific version dependency to ensure the code is safe
- exclusion of direct access to `__builtins__` via attributes or subscripts to prevent access to unsafe functions

I have some concerns about this approach.

First, it doesn't work - it's vulnerable to monkey-patching.
Namely, code injection is possible if a whitelisted named function is modified to produce side-effects.

Given that part of the code definition will need users to allow execution from a whitelisted namespace, this isn't introducing a new attack vector per se.
The security model here is allowing a user who has access to the file-system via one method to access it via another.

More importantly I think, it doesn't scale well.
This is partially taken care of by allowing the user to create a whitelist namespace.
But this is an inelegant solution and will require sohpisticated python users who undertake some amount of work to set up the environment.
It is unlikely to scale well across configuration modes.