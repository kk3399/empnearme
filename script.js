$(document).ready(function(){
    $('select').formSelect();
    $('.modal').modal();
    $('.fixed-action-btn').floatingActionButton();
});

function onSearcByCriteriaChange(value) {
    console.log(value);
    var employerElement = document.getElementById("employer_div");
    employerElement.classList.toggle("hide");

    var milesFromElement = document.getElementById("miles_from_div");
    milesFromElement.classList.toggle("hide");

    var zipCodeElement = document.getElementById("zip_code_div");
    zipCodeElement.classList.toggle("hide");
}

function onEmployerInput(employer) {
    console.log(employer);
}

function onMilesFromInput(miles) {
    console.log(miles);
}

function onZipCodeInput(zipCode) {
    console.log(zipCode);
}

function onSalaryRangeInput(range) {
    console.log(range);
}

function onJobTitleInput(title) {
    console.log(title);
}

function onSwitch(value) {
    console.log(value);
}

function onSubmit() {

}