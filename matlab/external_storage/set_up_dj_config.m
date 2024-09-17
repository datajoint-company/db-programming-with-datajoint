%% 
% This notebook introduce how to set up the external storage path in the 
% configuration variable |dj.config.|You could check the default of |dj.config| 
% by:

dj.config
%% 
% Now you can set up the host, user, and the password

dj.config('databaseHost', 'host_name')
dj.config('databaseUser', 'user_name')
dj.config('databasePassword', 'password')
%% 
% We next would like to set up the external location. As an example, let's 
% set up an external location for calcium imaging, which is named as |imaging|.

imaging_storage = struct('protocol', 'file',...
                         'location', '~/imaging');
dj.config('stores.imaging', imaging_storage)
%% 
% Now the location has already set up. We could check the storage location 
% by:

dj.config('stores.imaging')
%% 
% We could save this config either locally with 

dj.config.saveLocal()
%% 
% Or save globally with 

dj.config.saveGlobal()
%% 
% Now let's create a schema and a table to test this location.

dj.createSchema
%% 
% give a name to this new schema, for example |external|, and link it to 
% a package named |+external|. Then inside the schema, create a table called |external.Blob| 
% by

dj.new
%% 
% In the file |+external/Blob.m|, we could see the definition of an external 
% field to be |external_blob: blob@imaging|. |imaging| is the name of the storage 
% we defined above. Now let's try insert 1 entry into this table:

insert(external.Blob, struct('id', 1, 'external_blob', rand(10)))
%% 
%  | |Check the table contents:

external.Blob
%% 
% Now fetch the |external_blob |as usual:

fetch1(external.Blob, 'external_blob')
%% 
% To the user, it feels like the field is still stored in the database, 
% but actually it is in the external storage, in this case ~/imaging.
% 
% If deleting the entry with |del| , the entry deleted is no longer fetchable, 
% but the external files are not deleted unless you clean it up manually.

del(external.Blob)
%% 
% To clean up, use the following command:

% get the schema object
schema = external.getSchema();
schema.external.table('imaging').delete(true,[])
%% 
% 1st delete argument is bool for deleting from external source
% 
% 2nd delete argument is a number for a delete limit (optional)