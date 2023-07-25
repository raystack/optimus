"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[7181],{3905:(e,t,r)=>{r.d(t,{Zo:()=>u,kt:()=>m});var n=r(7294);function o(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function c(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function a(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?c(Object(r),!0).forEach((function(t){o(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):c(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function s(e,t){if(null==e)return{};var r,n,o=function(e,t){if(null==e)return{};var r,n,o={},c=Object.keys(e);for(n=0;n<c.length;n++)r=c[n],t.indexOf(r)>=0||(o[r]=e[r]);return o}(e,t);if(Object.getOwnPropertySymbols){var c=Object.getOwnPropertySymbols(e);for(n=0;n<c.length;n++)r=c[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(o[r]=e[r])}return o}var p=n.createContext({}),i=function(e){var t=n.useContext(p),r=t;return e&&(r="function"==typeof e?e(t):a(a({},t),e)),r},u=function(e){var t=i(e.components);return n.createElement(p.Provider,{value:t},e.children)},l="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},f=n.forwardRef((function(e,t){var r=e.components,o=e.mdxType,c=e.originalType,p=e.parentName,u=s(e,["components","mdxType","originalType","parentName"]),l=i(r),f=o,m=l["".concat(p,".").concat(f)]||l[f]||d[f]||c;return r?n.createElement(m,a(a({ref:t},u),{},{components:r})):n.createElement(m,a({ref:t},u))}));function m(e,t){var r=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var c=r.length,a=new Array(c);a[0]=f;var s={};for(var p in t)hasOwnProperty.call(t,p)&&(s[p]=t[p]);s.originalType=e,s[l]="string"==typeof e?e:o,a[1]=s;for(var i=2;i<c;i++)a[i]=r[i];return n.createElement.apply(null,a)}return n.createElement.apply(null,r)}f.displayName="MDXCreateElement"},509:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>p,contentTitle:()=>a,default:()=>d,frontMatter:()=>c,metadata:()=>s,toc:()=>i});var n=r(7462),o=(r(7294),r(3905));const c={},a="Project",s={unversionedId:"concepts/project",id:"concepts/project",title:"Project",description:"A project/tenant represents a group of jobs, resources, and a scheduler with the specified configurations and infrastructure.",source:"@site/docs/concepts/project.md",sourceDirName:"concepts",slug:"/concepts/project",permalink:"/optimus/docs/concepts/project",draft:!1,editUrl:"https://github.com/raystack/optimus/edit/master/docs/docs/concepts/project.md",tags:[],version:"current",lastUpdatedBy:"Ravi Suhag",lastUpdatedAt:1690315961,formattedLastUpdatedAt:"Jul 25, 2023",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Architecture",permalink:"/optimus/docs/concepts/architecture"},next:{title:"Namespace",permalink:"/optimus/docs/concepts/namespace"}},p={},i=[],u={toc:i},l="wrapper";function d(e){let{components:t,...r}=e;return(0,o.kt)(l,(0,n.Z)({},u,r,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("h1",{id:"project"},"Project"),(0,o.kt)("p",null,"A project/tenant represents a group of jobs, resources, and a scheduler with the specified configurations and infrastructure.\nA project contains multiple user-created namespaces, and each has various jobs and resources."))}d.isMDXComponent=!0}}]);