## Let's think big

This follows on from our [AI TDD experiment](https://github.com/illmade-knight/ai-tdd)
In our examples, no doubt for simplicity, the projects start small and expand outwards.
That's good for the TDD cycle I guess.

Of course that's not how you think of a product 
(or maybe it is, and maybe this is a problem with software development that gives rise to feature creep...).

Instead let's define an overall 'vision' and start to break it down into parts.
Then maybe we get back to TDD but with it being led by the overall 'vision'

The current state is evolving - at present we have 3 microservices 
1) ingestion from devices
2) storage of raw device messages in slow recall format for audit
3) storage of decoded device messages in google's BigQuery for analysis

And a [service manager](services-mvp/servicemanager) 
which sets up elements for use by the other services - 
in future we'll use it to monitor the services as well.

we'll keep an evaluation of the overall process in [current state](current.md)

and we'll also keep a record of [problems](problems.md) we encounter.

here we'll set out the:

## Big picture

We'll outline a scenario and then role play through the entire lifecycle of a solution.
The AI (Gemini in our case but we can experiment with others) gets to see all of this 
Building the context is important for the agent and we find that as we progress the agent gets
better at anticipating our requirements (with a few nudges).

## Role play

We are a small consulting company that analyses client requirements and puts together software solutions to address 
those client requirements. 

Recently we have started incorporating IoT hardware into our portfolio and now offer more
mixed hardware and software solutions. Our clients have a need to monitor their electricity requirements and we are building
solutions using hardware and software to address these requirements.

### Client A needs:
The client has two main goals for the project

1) The client wants to reduce overall electricity costs
2) The client needs to monitor carbon emissions as part of upcoming legal requirements

We've thought of some requirements in advance but lets ask for some suggestions and compare 
````ai prompt
can you turn these client needs into some specifice requirements?
````

### Client A requirements:

Gemini responds with:

````ai response

Client A has two main goals:

1.  **Reduce Overall Electricity Costs:**
    * **Real-time Monitoring:** Collect and display real-time and historical electricity usage (power demand kW, energy consumption kWh) via a dashboard. This includes data from main and sub-meters with good granularity (facility-wide, specific areas, large equipment). All data needs to be logged.
    * **Peak Demand Management:** Identify peak demand periods, alert when approaching peak thresholds, allow input of complex electricity tariffs, and calculate costs based on these tariffs. The system should also offer insights into shifting loads to off-peak times.
    * **Anomaly Detection:** Automatically detect unusual consumption patterns (e.g., high usage off-hours, equipment using too much power) and alert users.
    * **Reporting & Analytics:** Generate reports on consumption and costs, allow comparisons, and enable data export for further analysis.

2.  **Monitor Carbon Emissions for Legal Requirements:**
    * **Emission Calculation:** Calculate CO2 equivalent (CO2e) emissions from electricity usage, using configurable (and potentially time-varying) emission factors. This will initially focus on Scope 2 emissions.
    * **Compliance Reporting:** Generate reports suitable for legal requirements, including total emissions, breakdowns, emission factors used, and underlying consumption data. An audit trail is necessary.
    * **Emission Reduction Insights:** Allow setting and tracking progress towards internal emission reduction targets and show how cost-saving measures impact emissions.

**General System-Wide (Non-Functional) Requirements:**
The system also needs to:
* Integrate with specified IoT hardware.
* Ensure data integrity and accuracy.
* Be scalable for more devices, data, and sites.
* Be secure (data encryption, role-based access control, secure authentication).
* Have a user-friendly and accessible UI.
* Be reliable and highly available (e.g., 99.9% uptime).
* Be maintainable with good documentation.
* Consider future interoperability with other systems.

This covers the main points from the detailed requirements document. Let me know if you'd like me to elaborate on any specific part!
````

OK thanks gemini, one of our consultants had a quick call with the client and noted down some specific points
they'd like included in the requirements. These overlap with what you've outlined - can you incorporate them together and
clean them up to make it a coherent set of requirements?

````client requirements
The client has identified a number of requirements that will help them achieve their goals. 
These requirements are all built around the accurate monitoring of electricity usage across their operations. 
Details of the implementation are left up to us.

1) The monitoring should be able to break down usage to locations and within locations to usage categories: 
HVAC, lighting, compute, etc.

2) Analysis of the data should be able to identify trends in usage over different time periods.

3) Data should also be available to Tableau or similar tools such as Google's Looker Studio

4) Data should also be available to simple tools like Google Sheets or Excel for presentations etc

5) A visual interface, web and/or app based is required to monitor some of this analysis - 
(typically once a useful visualization is identified in Tableau a service is then created to make this data insight generally available )

6) A reporting system should send a synopsis of usage to different user groups, 
some groups require detailed analysis and others only require reports in the case of specific conditions (faults, overuse etc...)
````

the response is in the [client requirements doc](clientrequirements.md)

### OK enough to go on with
That's pretty comprehensive - we want to get building a minimal viable product.
Our MVP needs to demonstrate that we can start to address the client's needs.

We'll look first at some System Requirements on our side and our first thought is that the system breaks down into 2 major parts

1) Data gathering and storage
2) Data analysis and reporting

Remember this is just a MVP so we'll simplify for now.

Here are some thoughts for the implementation - some may be over specific but we're thinking out loud and trying 
to get a feel for the solution

For the data gathering we'll start with a single energy monitor that reports every 2 minutes over MQTT.
We'll need to ingest this data in its native format and store it. 

Our inclination is to adopt a microservices architecture - by doing this we can add or remove energy monitors from our codebase
(some devices may become unavailable or better/cheaper alternatives may come along)

We can also create microservices based off insights created by our data analysts that supply data to the monitoring services etc

We'll want to store 'raw' data from the devices for audit but also process data to a unified format for analysis and display.

The raw data can be stored in a slow recal format as we don't need rapid access to it.

We'll also use protocol buffers to move data around - this will allow us to generate handlers for the data in a 
large number of computer languages and so teams working on the microservices can adopt the best approach for their specific tasks.

OK analyse this and see what you think?

````ai response
we've placed the response in 
````
[systeminitial](systeminitial.md)

OK, Gemini doesn't think it's a complete non-starter 
(though the sycophantic remarks seem a bit generic so it's hard to tell how honest it's being some of the time)

let's go to [services-mvp](services-mvp) to pick things up - 

we'll do our initial 'versioning' in the open so we can follow things without switching branches etc...