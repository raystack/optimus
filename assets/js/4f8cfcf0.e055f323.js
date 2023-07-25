"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[8210],{3905:(e,t,n)=>{n.d(t,{Zo:()=>s,kt:()=>f});var r=n(7294);function o(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function i(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function a(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?i(Object(n),!0).forEach((function(t){o(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function l(e,t){if(null==e)return{};var n,r,o=function(e,t){if(null==e)return{};var n,r,o={},i=Object.keys(e);for(r=0;r<i.length;r++)n=i[r],t.indexOf(n)>=0||(o[n]=e[n]);return o}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(r=0;r<i.length;r++)n=i[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(o[n]=e[n])}return o}var c=r.createContext({}),p=function(e){var t=r.useContext(c),n=t;return e&&(n="function"==typeof e?e(t):a(a({},t),e)),n},s=function(e){var t=p(e.components);return r.createElement(c.Provider,{value:t},e.children)},u="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},m=r.forwardRef((function(e,t){var n=e.components,o=e.mdxType,i=e.originalType,c=e.parentName,s=l(e,["components","mdxType","originalType","parentName"]),u=p(n),m=o,f=u["".concat(c,".").concat(m)]||u[m]||d[m]||i;return n?r.createElement(f,a(a({ref:t},s),{},{components:n})):r.createElement(f,a({ref:t},s))}));function f(e,t){var n=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var i=n.length,a=new Array(i);a[0]=m;var l={};for(var c in t)hasOwnProperty.call(t,c)&&(l[c]=t[c]);l.originalType=e,l[u]="string"==typeof e?e:o,a[1]=l;for(var p=2;p<i;p++)a[p]=n[p];return r.createElement.apply(null,a)}return r.createElement.apply(null,n)}m.displayName="MDXCreateElement"},2639:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>c,contentTitle:()=>a,default:()=>d,frontMatter:()=>i,metadata:()=>l,toc:()=>p});var r=n(7462),o=(n(7294),n(3905));const i={},a="Uploading Job to Scheduler",l={unversionedId:"client-guide/uploading-jobs-to-scheduler",id:"client-guide/uploading-jobs-to-scheduler",title:"Uploading Job to Scheduler",description:"Compile and upload all jobs in the project by using this command:",source:"@site/docs/client-guide/uploading-jobs-to-scheduler.md",sourceDirName:"client-guide",slug:"/client-guide/uploading-jobs-to-scheduler",permalink:"/optimus/docs/client-guide/uploading-jobs-to-scheduler",draft:!1,editUrl:"https://github.com/raystack/optimus/edit/master/docs/docs/client-guide/uploading-jobs-to-scheduler.md",tags:[],version:"current",lastUpdatedBy:"Ravi Suhag",lastUpdatedAt:1690315961,formattedLastUpdatedAt:"Jul 25, 2023",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Applying Job Specifications",permalink:"/optimus/docs/client-guide/applying-job-specifications"},next:{title:"Organizing Specifications",permalink:"/optimus/docs/client-guide/organizing-specifications"}},c={},p=[],s={toc:p},u="wrapper";function d(e){let{components:t,...n}=e;return(0,o.kt)(u,(0,r.Z)({},s,n,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("h1",{id:"uploading-job-to-scheduler"},"Uploading Job to Scheduler"),(0,o.kt)("p",null,"Compile and upload all jobs in the project by using this command:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus scheduler upload-all\n")),(0,o.kt)("p",null,(0,o.kt)("em",{parentName:"p"},"Note: add --config flag if you are not in the same directory with your client configuration (optimus.yaml).")),(0,o.kt)("p",null,"This command will compile all of the jobs in the project to Airflow DAG files and will store the result to the path\nthat has been set as ",(0,o.kt)("inlineCode",{parentName:"p"},"STORAGE_PATH")," in the project configuration. Do note that ",(0,o.kt)("inlineCode",{parentName:"p"},"STORAGE")," secret might be needed if\nthe storage requires a credential."),(0,o.kt)("p",null,"Once you have the DAG files in the storage, you can sync the files to Airflow as you\u2019d like."))}d.isMDXComponent=!0}}]);