using Documenter

const page_rename = Dict("developer.md" => "Developer docs") # Without the numbers
const numbered_pages = [
  file for file in readdir(joinpath(@__DIR__, "src")) if
  file != "index.md" && splitext(file)[2] == ".md"
]

makedocs(;
  modules=Module[],
  authors="",
  repo="https://github.com/TulipaEnergy/Tulipa-OBZ-CaseStudy.jl/blob/{commit}{path}#{line}",
  sitename="Tulipa-OBZ-CaseStudy.jl",
  format=Documenter.HTML(;
    canonical="https://TulipaEnergy.github.io/Tulipa-OBZ-CaseStudy.jl",
  ),
  pages=["index.md"; numbered_pages],
)

deploydocs(; repo="github.com/TulipaEnergy/Tulipa-OBZ-CaseStudy.jl")
