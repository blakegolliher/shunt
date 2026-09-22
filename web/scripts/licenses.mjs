import { readFile, readdir, writeFile } from 'node:fs/promises'
import path from 'node:path'

const root = path.resolve(import.meta.dirname, '..')
const lock = JSON.parse(await readFile(path.join(root, 'package-lock.json'), 'utf8'))
const packages = []

async function licenseFiles(directory, recursive = false) {
  const found = []
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    if (entry.isFile() && /^(?:licen[cs]e|copying)(?:\.|$)/i.test(entry.name)) found.push(path.join(directory, entry.name))
    else if (recursive && entry.isDirectory() && entry.name !== 'node_modules') found.push(...await licenseFiles(path.join(directory, entry.name), true))
  }
  return found.sort()
}

for (const [location, metadata] of Object.entries(lock.packages)) {
  if (!location.startsWith('node_modules/') || metadata.dev === true) continue
  const directory = path.join(root, location)
  const manifest = JSON.parse(await readFile(path.join(directory, 'package.json'), 'utf8'))
  const license = String(manifest.license ?? metadata.license ?? '')
  if (!license) throw new Error(`${manifest.name} has no declared license`)
  if (/\b(?:A?GPL|LGPL|MPL|EPL|CDDL|OSL)\b/i.test(license)) throw new Error(`${manifest.name} has copyleft license ${license}`)
  let files = await licenseFiles(directory)
  if (files.length === 0) files = await licenseFiles(directory, true)
  if (files.length === 0) throw new Error(`${manifest.name} has no license file in its installed package`)
  const texts = []
  for (const file of files) {
    const licenseText = (await readFile(file, 'utf8')).replaceAll('\r\n', '\n').split('\n').map((line) => line.trimEnd()).join('\n')
    texts.push(`${path.relative(directory, file)}\n${licenseText}`)
  }
  packages.push({ name: manifest.name, version: manifest.version, license, text: texts.join('\n') })
}
packages.sort((a, b) => a.name.localeCompare(b.name))
let output = 'shunt web production dependencies — generated from package-lock.json by npm run licenses\n'
for (const item of packages) {
  output += `\n--------------------------------------------------------------------------------\n${item.name}@${item.version} (${item.license})\n--------------------------------------------------------------------------------\n${item.text.trim()}\n`
}
await writeFile(path.join(root, 'THIRD_PARTY_NOTICES'), output)
console.log(`web/THIRD_PARTY_NOTICES: ${packages.length} production packages; no copyleft licenses`)
