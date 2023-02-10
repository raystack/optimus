"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[2492],{3905:function(e,t,n){n.d(t,{Zo:function(){return p},kt:function(){return m}});var r=n(7294);function a(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){a(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,r,a=function(e,t){if(null==e)return{};var n,r,a={},o=Object.keys(e);for(r=0;r<o.length;r++)n=o[r],t.indexOf(n)>=0||(a[n]=e[n]);return a}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(r=0;r<o.length;r++)n=o[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(a[n]=e[n])}return a}var l=r.createContext({}),c=function(e){var t=r.useContext(l),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},p=function(e){var t=c(e.components);return r.createElement(l.Provider,{value:t},e.children)},u={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},d=r.forwardRef((function(e,t){var n=e.components,a=e.mdxType,o=e.originalType,l=e.parentName,p=s(e,["components","mdxType","originalType","parentName"]),d=c(n),m=a,g=d["".concat(l,".").concat(m)]||d[m]||u[m]||o;return n?r.createElement(g,i(i({ref:t},p),{},{components:n})):r.createElement(g,i({ref:t},p))}));function m(e,t){var n=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var o=n.length,i=new Array(o);i[0]=d;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s.mdxType="string"==typeof e?e:a,i[1]=s;for(var c=2;c<o;c++)i[c]=n[c];return r.createElement.apply(null,i)}return r.createElement.apply(null,n)}d.displayName="MDXCreateElement"},281:function(e,t,n){n.r(t),n.d(t,{frontMatter:function(){return s},contentTitle:function(){return l},metadata:function(){return c},toc:function(){return p},default:function(){return d}});var r=n(7462),a=n(3366),o=(n(7294),n(3905)),i=["components"],s={id:"create-bigquery-external-table",title:"Create bigquery external table"},l=void 0,c={unversionedId:"guides/create-bigquery-external-table",id:"guides/create-bigquery-external-table",isDocsHomePage:!1,title:"Create bigquery external table",description:"A BigQuery external table is a data source stored in external storage that you can query directly",source:"@site/docs/guides/create-bigquery-external-table.md",sourceDirName:"guides",slug:"/guides/create-bigquery-external-table",permalink:"/optimus/docs/guides/create-bigquery-external-table",editUrl:"https://github.com/odpf/optimus/edit/master/docs/docs/guides/create-bigquery-external-table.md",tags:[],version:"current",lastUpdatedBy:"sravankorumilli",lastUpdatedAt:1676007626,formattedLastUpdatedAt:"2/10/2023",frontMatter:{id:"create-bigquery-external-table",title:"Create bigquery external table"},sidebar:"docsSidebar",previous:{title:"Create bigquery view",permalink:"/optimus/docs/guides/create-bigquery-view"},next:{title:"Organising specifications",permalink:"/optimus/docs/guides/organising-specifications"}},p=[{value:"Creating external table with Optimus",id:"creating-external-table-with-optimus",children:[]},{value:"Creating external table over REST",id:"creating-external-table-over-rest",children:[]},{value:"Creating external table over GRPC",id:"creating-external-table-over-grpc",children:[]}],u={toc:p};function d(e){var t=e.components,n=(0,a.Z)(e,i);return(0,o.kt)("wrapper",(0,r.Z)({},u,n,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("p",null,"A BigQuery external table is a data source stored in external storage that you can query directly\nin BigQuery the same way you query a table. You can specify the schema of the external table when\nit is created. At the moment only Google Drive source with Google Sheets format is supported."),(0,o.kt)("p",null,"There are 3 ways to create an external table:"),(0,o.kt)("h3",{id:"creating-external-table-with-optimus"},"Creating external table with Optimus"),(0,o.kt)("p",null,"Supported datastore can be selected by calling"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-bash"},"optimus resource create\n")),(0,o.kt)("p",null,"Optimus will request a resource name which should be unique across whole datastore.\nAll resource specification contains a name field which conforms to a fixed format.\nIn case of bigquery external table, format should be\n",(0,o.kt)("inlineCode",{parentName:"p"},"projectname.datasetname.tablename"),".\nAfter the name is provided, ",(0,o.kt)("inlineCode",{parentName:"p"},"optimus")," will create a file in configured datastore\ndirectory. Open the created specification file and add additional spec details\nas follows:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-yaml"},'version: 1\nname: temporary-project.optimus-playground.first_table\ntype: external_table\nlabels:\n  usage: testexternaltable\n  owner: optimus\nspec:\n  description: "example description"\n  schema:\n    - name: colume1\n      type: INTEGER\n    - name: colume2\n      type: TIMESTAMP\n      description: "example field 2"\n  source:\n    type: google_sheets\n    uris:\n      - https://docs.google.com/spreadsheets/d/spreadsheet_id\n    config:\n      range: Sheet1!A1:B4 # Range of data to be ingested in format of [Sheet Name]![Cell Range]\n      skip_leading_rows: 1 # Row of records to skip\n')),(0,o.kt)("p",null,"This will add labels, description, schema, and external table source specification depending\non the type of external table."),(0,o.kt)("p",null,"Optimus generates specification on the root directory inside datastore with directory\nname same as resource name, although you can change directory name to whatever you\nfind fit to organize resources. Directory structures inside datastore doesn't\nmatter as long as ",(0,o.kt)("inlineCode",{parentName:"p"},"resource.yaml")," is in a unique directory."),(0,o.kt)("p",null,"For example following is a valid directory structure"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-shell"},"./\n./bigquery/temporary-project/optimus-playground/resource.yaml\n./bigquery/temporary-project/optimus-playground/first_external_table/resource.yaml\n")),(0,o.kt)("h3",{id:"creating-external-table-over-rest"},"Creating external table over REST"),(0,o.kt)("p",null,"Optimus exposes Create/Update rest APIS"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},"Create: POST /api/v1beta1/project/{project_name}/namespace/{namespace}/datastore/{datastore_name}/resource\nUpdate: PUT /api/v1beta1/project/{project_name}/namespace/{namespace}/datastore/{datastore_name}/resource\nRead: GET /api/v1beta1/project/{project_name}/namespace/{namespace}/datastore/{datastore_name}/resource/{resource_name}\n")),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-json"},'{\n  "resource": {\n    "version": 1,\n    "name": "temporary-project.optimus-playground.first_table",\n    "datastore": "bigquery",\n    "type": "external_table",\n    "labels": {\n      "usage": "testexternaltable",\n      "owner": "optimus"\n    },\n    "spec": {\n      "description": "example description",\n      "schema": [\n        {\n          "name": "column1",\n          "type": "INTEGER"\n        },\n        {\n          "name": "column2",\n          "type": "TIMESTAMP",\n          "description": "example description",\n          "mode": "required"\n        }\n      ],\n      "source": {\n        "type": "google_sheets",\n        "uris": ["https://docs.google.com/spreadsheets/d/spreadsheet_id"],\n        "config": {\n          "range": "Sheet1!A1:B4",\n          "skip_leading_rows": 1\n        }\n      }\n    }\n  }\n}\n')),(0,o.kt)("h3",{id:"creating-external-table-over-grpc"},"Creating external table over GRPC"),(0,o.kt)("p",null,"Optimus in RuntimeService exposes an RPC"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-protobuf"},"rpc CreateResource(CreateResourceRequest) returns (CreateResourceResponse) {}\n\nmessage CreateResourceRequest {\n    string project_name = 1;\n    string datastore_name = 2;\n    ResourceSpecification resource = 3;\n    string namespace = 4;\n}\n")),(0,o.kt)("p",null,"Function payload should be self-explanatory other than the struct ",(0,o.kt)("inlineCode",{parentName:"p"},"spec")," part which\nis very similar to how json representation look."),(0,o.kt)("p",null,"Spec will use ",(0,o.kt)("inlineCode",{parentName:"p"},"structpb")," struct created with ",(0,o.kt)("inlineCode",{parentName:"p"},"map[string]interface{}"),"\nFor example:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-go"},'map[string]interface{\n    "description": "example description",\n    "schema": []interface{\n        map[string]interface{\n            "name": "colume1",\n            "type": "integer"\n        },\n        map[string]interface{\n            "name": "colume2",\n            "type": "timestamp"\n            "description": "some description",\n            "mode": "required"\n        },\n    },\n    "source": map[string]interface{\n        "type": "google_sheets",\n        "uris": []string{"https://docs.google.com/spreadsheets/d/spreadsheet_id"},\n        "config": map[string]interface{\n            "range": "Sheet1!A1:B4",\n            "skip_leading_rows": 1\n        }\n    },\n}\n')))}d.isMDXComponent=!0}}]);