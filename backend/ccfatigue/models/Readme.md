Database design
---------------

For the database I realized late that it was probably something more involuted that you had in mind.

Here I created a table for all the separated concerns that are described in the example excel files.

In particular, it seemed reasonable to separate everything that was treated as different in the 
excel metadata (e.g. the publication, or the instrument).

What is missing is a bit more of interconnection between them, with relationships to be added between databases,
and relative checks if an experiment does not have the relative test metadata.

Required fields are selected as such from the metadata example.

I could not find a reasonable good usecase for the units database. The only place where they are defined is in the
general excel, and never in the single experiment files; if this was the case, I could have created a subroutine to 
check the experiment units VS the master units (e.g., a list of suffixes to check cm vs mm and assign a scale to convert it
before insertion).

  


