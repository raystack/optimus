"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[2743],{3905:(e,t,n)=>{n.d(t,{Zo:()=>p,kt:()=>m});var a=n(7294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function i(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function s(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?i(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function o(e,t){if(null==e)return{};var n,a,r=function(e,t){if(null==e)return{};var n,a,r={},i=Object.keys(e);for(a=0;a<i.length;a++)n=i[a],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(a=0;a<i.length;a++)n=i[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var l=a.createContext({}),c=function(e){var t=a.useContext(l),n=t;return e&&(n="function"==typeof e?e(t):s(s({},t),e)),n},p=function(e){var t=c(e.components);return a.createElement(l.Provider,{value:t},e.children)},u="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},h=a.forwardRef((function(e,t){var n=e.components,r=e.mdxType,i=e.originalType,l=e.parentName,p=o(e,["components","mdxType","originalType","parentName"]),u=c(n),h=r,m=u["".concat(l,".").concat(h)]||u[h]||d[h]||i;return n?a.createElement(m,s(s({ref:t},p),{},{components:n})):a.createElement(m,s({ref:t},p))}));function m(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var i=n.length,s=new Array(i);s[0]=h;var o={};for(var l in t)hasOwnProperty.call(t,l)&&(o[l]=t[l]);o.originalType=e,o[u]="string"==typeof e?e:r,s[1]=o;for(var c=2;c<i;c++)s[c]=n[c];return a.createElement.apply(null,s)}return a.createElement.apply(null,n)}h.displayName="MDXCreateElement"},2440:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>l,contentTitle:()=>s,default:()=>d,frontMatter:()=>i,metadata:()=>o,toc:()=>c});var a=n(7462),r=(n(7294),n(3905));const i={},s=void 0,o={unversionedId:"rfcs/secret_management",id:"rfcs/secret_management",title:"secret_management",description:"- Feature Name: Secret Management",source:"@site/docs/rfcs/20211002_secret_management.md",sourceDirName:"rfcs",slug:"/rfcs/secret_management",permalink:"/optimus/docs/rfcs/secret_management",draft:!1,editUrl:"https://github.com/raystack/optimus/edit/master/docs/docs/rfcs/20211002_secret_management.md",tags:[],version:"current",lastUpdatedBy:"Ravi Suhag",lastUpdatedAt:1690315961,formattedLastUpdatedAt:"Jul 25, 2023",sidebarPosition:20211002,frontMatter:{}},l={},c=[{value:"Using secrets",id:"using-secrets",level:4},{value:"Authentication &amp; Authorization",id:"authentication--authorization",level:4},{value:"Optimus CLI",id:"optimus-cli",level:3},{value:"Create/Update",id:"createupdate",level:4},{value:"Delete",id:"delete",level:4},{value:"List",id:"list",level:4},{value:"Using secrets without Optimus",id:"using-secrets-without-optimus",level:3},{value:"Rotating Optimus Server key",id:"rotating-optimus-server-key",level:3},{value:"Migration",id:"migration",level:4}],p={toc:c},u="wrapper";function d(e){let{components:t,...n}=e;return(0,r.kt)(u,(0,a.Z)({},p,n,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Feature Name: Secret Management"),(0,r.kt)("li",{parentName:"ul"},"Status: Approved"),(0,r.kt)("li",{parentName:"ul"},"Start Date: 2021-10-02"),(0,r.kt)("li",{parentName:"ul"},"Authors: Kush Sharma & Sravan ")),(0,r.kt)("h1",{id:"summary"},"Summary"),(0,r.kt)("p",null,"A lot of transformation operations require credentials to execute, there is a need to have a convenient way to save secrets and then access them in containers during the execution. This secret may also be needed in plugin adapters to compute dependencies/compile assets/etc before the actual transformation even begin. This is currently done using registering a secret to optimus so that it can be accessed by plugins and Kubernetes opaque secret, a single secret per plugin, getting mounted in the container(i.e. not at individual job level)."),(0,r.kt)("p",null,"This can be solved by allowing users to register secret from Optimus CLI as a key value pair, storing them encrypted using a single key across all tenants."),(0,r.kt)("h1",{id:"technical-design"},"Technical Design"),(0,r.kt)("p",null,"To keep string literals as secret, it is a requirement Optimus keep them encrypted in database. Optimus Server Key is used to encrypt & decrypt the secret & will ensure the secret is encrypted at rest. Each secret is a key value pair where key is an alpha numeric literal and value is base64 encoded string. "),(0,r.kt)("p",null,"Optimus has two sets of secrets, user managed secrets & others which are needed for server operations. Each of server managed secrets should be prefixed by ",(0,r.kt)("inlineCode",{parentName:"p"},"_OPTIMUS_<key name>")," and will not be allowed to be used by users in the job spec. Optimus should also disallow anyone using this prefix to register their secrets. The secrets can be namespaced by optimus namespace or at project level, and will be accessible accordingly. Secret names should be maintained unique across a project."),(0,r.kt)("h4",{id:"using-secrets"},"Using secrets"),(0,r.kt)("p",null,"Secrets can be used as part of the job spec config using macros with their names. This will work as aliasing the secret to be used in containers. Only the secrets created at project & namespace the job belongs to can be referenced. So, for the plugin writers any secret that plugin needs can be accessed through environment variables defined in the job spec or can get the secrets by defining in any assets."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-yaml"},"task: foo\nconfig:\n  do: this\n  dsn: {{ .secret.postgres_dsn }}\n")),(0,r.kt)("p",null,"One thing to note is currently we print all the container environment variables using ",(0,r.kt)("inlineCode",{parentName:"p"},"printenv")," command as debug. This should be removed after this RFC is merged to avoid exposing secrets in container logs."),(0,r.kt)("p",null,"Only the admins & containers to be authorized for ",(0,r.kt)("inlineCode",{parentName:"p"},"registerinstance")," end point, as this will allow access to all secrets."),(0,r.kt)("p",null,"Because Optimus is deployed in trusted network, we don't need TLS for now to fetch job secrets but once Optimus is deployed as a service on edge network, this communication should only happen over TLS. "),(0,r.kt)("h4",{id:"authentication--authorization"},"Authentication & Authorization"),(0,r.kt)("p",null,"Even though Optimus doesn't have its own authentication, expect users to bring in their own auth proxy infront of Optimus. All user access & container access will be restricted through the auth proxy. The corresponding secret through which the containers running in the kubernetes cluster will be authenticated need to be precreated per project."),(0,r.kt)("h3",{id:"optimus-cli"},"Optimus CLI"),(0,r.kt)("p",null,"User interaction to manage a secret will start from CLI. Users can create/update/list/delete a secret as follows"),(0,r.kt)("p",null,"By default secrets will be created under their namespace, but optionally the secret can be created at project level by not providing any namespace while creation. This is needed if users want to allow access across entire project."),(0,r.kt)("p",null,"Secrets can be accessed by providing the project & namespace the secret is created in, if the secret is created at project level then namespace can be set to empty string if optimus.yaml already has the namespace configured."),(0,r.kt)("h4",{id:"createupdate"},"Create/Update"),(0,r.kt)("p",null,(0,r.kt)("inlineCode",{parentName:"p"},"optimus secret create/update <name> <value> ")," will take a secret name and value"),(0,r.kt)("p",null,(0,r.kt)("inlineCode",{parentName:"p"},'optimus secret create/update <name> --file="path"')," should read the file content as value. "),(0,r.kt)("p",null,"Additional flag ",(0,r.kt)("inlineCode",{parentName:"p"},"--base64")," can  be provided by user stating the value is already encoded, if not provided optimus ensures to encode & store it, basic checks can be done to check if the string is a valid base64 encoded string."),(0,r.kt)("h4",{id:"delete"},"Delete"),(0,r.kt)("p",null,(0,r.kt)("inlineCode",{parentName:"p"},"optimus secret delete <name>")," "),(0,r.kt)("h4",{id:"list"},"List"),(0,r.kt)("p",null,(0,r.kt)("inlineCode",{parentName:"p"},"optimus secret list")," to list all created secrets in a project/namespace, along with the creation/updated time, will be helpful such that users can use in the job spec, as users might forget the key name, this will not list the system managed secrets."),(0,r.kt)("p",null,"List operation will print a digest of the secret. Digest should be a SHA hash of the encrypted string to simply visualize it as a signature when a secret is changed or the key gets rotated."),(0,r.kt)("p",null," Example:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre"},"     NAME     |              DIGEST              |  NAMESPACE |  DATE\n  SECRET_1    | 6c463e806738046ff3c78a08d8bd2b70 |     *      | 2021-10-06 02:02:02\n  SECRET_2    | 3aa788a21a76651c349ceeee76f1cb76 |   finance  | 2021-10-06 06:02:02\n  SECRET_2    | 3aa788a21a76651c349ceeee76f1cb76 |  transport | 2021-10-06 06:02:02\n")),(0,r.kt)("p",null,"This command will only shows the user managed secret sets and ignoring the system managed secret, while on the REST response\nboth sets can be shown. An additional field in secret table called 'TYPE' can be added to differentiate the two sets. "),(0,r.kt)("h3",{id:"using-secrets-without-optimus"},"Using secrets without Optimus"),(0,r.kt)("p",null,"If someone wants to pass an exclusive secret without registering it with Optimus first, that should also be possible. "),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"In case of k8s: this can be done using a new field introduced in Job spec as ",(0,r.kt)("inlineCode",{parentName:"li"},"metadata")," which will allow users to mount arbitrary secrets inside the container available in the same k8s namespace.")),(0,r.kt)("h3",{id:"rotating-optimus-server-key"},"Rotating Optimus Server key"),(0,r.kt)("p",null,"There is a need for rotating Optimus Server Key when it is compromised. As the server key is configured through environment variable, the rotation can happen by configuring through environment variables. There can be two environment variables for server keys ",(0,r.kt)("inlineCode",{parentName:"p"},"OLD_APP_KEY")," & ",(0,r.kt)("inlineCode",{parentName:"p"},"APP_KEY"),". During startup sha of the ",(0,r.kt)("inlineCode",{parentName:"p"},"OLD_APP_KEY")," is compared with the sha stored in the database, if it matches then rotation will happen and at the end of rotation the sha will be replaced with  ",(0,r.kt)("inlineCode",{parentName:"p"},"APP_KEY's")," sha. The comparision is needed to check to not attempt rotation during restarts. If there are multiple replicas then as we do this in a transaction only one succeeds."),(0,r.kt)("p",null,"This step will internally loading all the secrets that belong to a project to memory, decrypting it with the old_key, and encrypting it with the new key, the entire operation will happen in a single db transaction."),(0,r.kt)("h4",{id:"migration"},"Migration"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"This design will be a breaking change compare to how the secrets are handled and will require all the current secrets to be registered again.\nCurrent system managed secrets will be re-registered using ",(0,r.kt)("inlineCode",{parentName:"li"},"_OPTIMUS_")," prefix. Plugin secrets will also need to be registered to Optimus.")),(0,r.kt)("h1",{id:"footnotes--references"},"Footnotes & References"),(0,r.kt)("ul",null,(0,r.kt)("li",{parentName:"ul"},"Multi party encryption via ",(0,r.kt)("a",{parentName:"li",href:"https://github.com/FiloSottile/age"},"age")),(0,r.kt)("li",{parentName:"ul"},(0,r.kt)("a",{parentName:"li",href:"https://gocloud.dev/howto/secrets/"},"Key Management Services "))))}d.isMDXComponent=!0}}]);