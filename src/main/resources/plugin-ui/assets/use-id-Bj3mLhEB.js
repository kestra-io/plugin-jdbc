import{Pn as e,Rn as t,Ur as n}from"./_virtual_mf___mfe_internal__io_mf_3_kestra_mf_3_plugin_mf_3_jdbc__mf_owner__50325819103327__loadShare__shared-BpcjH7t3.js";import{o as r}from"./install-BhZ2ZsuQ.js";import{t as i}from"./error-DgvpZbwv.js";import{i as a,n as o}from"./dist-DQSiCQmx.js";var s={prefix:Math.floor(Math.random()*1e4),current:0},c=Symbol(`elIdInjection`),l=()=>e()?t(c,s):s,u=e=>{let t=l();!a&&t===s&&i(`IdInjection`,`Looks like you are using server rendering, you must provide a id provider to ensure the hydration process to be succeed
usage: app.provide(ID_INJECTION_KEY, {
  prefix: number,
  current: number,
})`);let c=r();return o(()=>n(e)||`${c.value}-id-${t.prefix}-${t.current++}`)};export{l as n,u as t};